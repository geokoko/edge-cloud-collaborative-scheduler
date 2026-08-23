#include <bits/stdc++.h>

enum class RequestStage {
    READY_P_PRE,
    RUNNING_P_PRE,
    WAITING_P_UP,
    READY_P_PROC,
    RUNNING_P_PROC,
    WAITING_P_DOWN,
    READY_P_POST,
    RUNNING_P_POST,
    READY_D_PRE,
    RUNNING_D_PRE,
    WAITING_D_UP,
    READY_D_PROC,
    RUNNING_D_PROC,
    WAITING_D_DOWN,
    READY_D_POST,
    RUNNING_D_POST,
    FINISHED,
};

enum class WorkKind { PREFILL, DECODE };
enum class TaskStep { PRE, PROC, POST };
enum class EventKind { ARRIVAL, TASK_DONE, TRANSFER_DONE, FINISH };
enum class Direction { UP, DOWN };
enum class TransferKind { PREFILL, DECODE };

struct TaskSpec {
    WorkKind work = WorkKind::PREFILL;
    TaskStep step = TaskStep::PRE;
    int remote = -1;
    int layer_start = -1;
    int layer_end = -1;
    std::vector<int> request_ids;

    bool operator==(const TaskSpec&) const = default;
};

struct Assignment {
    int server = -1;  // -1 is E, otherwise C<server>.
    TaskSpec task;
};

struct Event {
    EventKind kind = EventKind::ARRIVAL;
    int rid = -1;
    int input_length = 0;

    int server = -1;
    TaskSpec task;
    double duration = 0.0;

    Direction direction = Direction::UP;
    TransferKind transfer_kind = TransferKind::PREFILL;
    int remote = -1;
    std::int64_t size = 0;
    std::vector<int> request_ids;
};

struct QueuedTransfer {
    TransferKind kind = TransferKind::PREFILL;
    int remote = -1;
    std::int64_t size = 0;
    std::vector<int> request_ids;
    double expected_finish_time = 0.0;
};

struct Request {
    int rid = -1;
    int input_length = 0;
    int remote = -1;
    RequestStage stage = RequestStage::READY_P_PRE;
    double arrival_time = 0.0;
    double decode_ready_time = 0.0;
    bool finished = false;
    int next_prefill_layer = 0;
};

struct SystemConfig {
    int remote_count = 0;
    double schedule_cost = 0.0;
    double latency_in_ms = 0.0;
    double bandwidth_gbps = 0.0;
    int bytes_per_token = 0;
    int num_layers = 0;
};

struct ScoringConfig {
    double slo1 = 0.0;
    double slo2 = 0.0;
    double throughput_upper_bound = 0.0;
    double throughput_base = 0.0;
    double distance_base = 0.0;
    double throughput_weight = 0.0;
    double waiting_weight = 0.0;
};

struct TaskTimingRow {
    int batch_size = 0;
    double prefill_pre = 0.0;
    double prefill_proc = 0.0;
    double prefill_post = 0.0;
    double decode_pre = 0.0;
    double decode_proc = 0.0;
    double decode_post = 0.0;
};

struct ReadyKey {
    double priority_time = 0.0;
    int rid = -1;
};

struct ReadyKeyLess {
    bool operator()(const ReadyKey& lhs, const ReadyKey& rhs) const {
        return std::tie(lhs.priority_time, lhs.rid) <
               std::tie(rhs.priority_time, rhs.rid);
    }
};

class Scheduler {
public:
    Scheduler(std::istream& input, std::ostream& output)
        : input_(input), output_(output) {}

    bool readStartup() {
        if (!(input_ >> system_.remote_count >> system_.schedule_cost >>
              system_.latency_in_ms >> system_.bandwidth_gbps >>
              system_.bytes_per_token >> system_.num_layers)) {
            return false;
        }
        if (!(input_ >> scoring_.slo1 >> scoring_.slo2 >>
              scoring_.throughput_upper_bound >> scoring_.throughput_base >>
              scoring_.distance_base >> scoring_.throughput_weight >>
              scoring_.waiting_weight)) {
            return false;
        }

        int row_count = 0;
        if (!(input_ >> row_count) || row_count < 0) {
            return false;
        }
        task_times_.resize(static_cast<std::size_t>(row_count));
        for (TaskTimingRow& row : task_times_) {
            if (!(input_ >> row.batch_size >> row.prefill_pre >>
                  row.prefill_proc >> row.prefill_post >> row.decode_pre >>
                  row.decode_proc >> row.decode_post)) {
                return false;
            }
        }

        if (system_.remote_count <= 0 || system_.num_layers <= 0 ||
            system_.bytes_per_token <= 0 || system_.latency_in_ms <= 0.0 ||
            system_.bandwidth_gbps <= 0.0 || scoring_.slo1 <= 0.0 ||
            scoring_.slo2 <= 0.0) {
            return false;
        }
        if (!prepareTimingModel()) {
            return false;
        }
        const std::size_t remote_count =
            static_cast<std::size_t>(system_.remote_count);
        remote_busy_.assign(remote_count, false);
        yield_to_decode_.assign(static_cast<std::size_t>(system_.remote_count),
                                false);
        deferred_d_proc_.assign(remote_count, false);
        remote_tasks_.resize(remote_count);
        ready_p_proc_.resize(remote_count);
        ready_d_proc_.resize(remote_count);
        active_remote_requests_.assign(remote_count, 0);
        decode_active_by_remote_.assign(remote_count, 0);
        waiting_d_up_.assign(remote_count, 0);
        waiting_d_down_.assign(remote_count, 0);
        ready_d_pre_by_remote_.assign(remote_count, 0);
        decode_up_finishes_.resize(remote_count);
        input_.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        return true;
    }

    bool readAndProcessFrame() {
        std::string header;
        if (!std::getline(input_, header) || header == "END") {
            return false;
        }

        double timestamp = 0.0;
        if (!parseSingleValue(header, timestamp) || !std::isfinite(timestamp)) {
            return false;
        }

        std::string count_line;
        int event_count = 0;
        if (!std::getline(input_, count_line) ||
            !parseSingleValue(count_line, event_count) || event_count < 0) {
            return false;
        }

        std::vector<Event> events;
        events.reserve(static_cast<std::size_t>(event_count));
        for (int i = 0; i < event_count; ++i) {
            std::string line;
            if (!std::getline(input_, line)) {
                return false;
            }
            std::optional<Event> event = parseEvent(line);
            if (!event) {
                return false;
            }
            events.push_back(std::move(*event));
        }

        current_time_ = timestamp;
        for (const Event& event : events) {
            if (event.kind != EventKind::FINISH && !applyEvent(event, timestamp)) {
                return false;
            }
        }
        for (const Event& event : events) {
            if (event.kind == EventKind::FINISH && !applyFinish(event.rid)) {
                return false;
            }
        }

        std::vector<Assignment> assignments = chooseAssignments();
        emitAssignments(assignments);
        return static_cast<bool>(output_);
    }

    std::vector<Assignment> chooseAssignments() {
        std::vector<Assignment> assignments;
        assignments.reserve(static_cast<std::size_t>(system_.remote_count + 1));

        for (int remote = 0; remote < system_.remote_count; ++remote) {
            if (remote_busy_[static_cast<std::size_t>(remote)]) {
                continue;
            }

            TaskSpec task;
            bool found = false;
            const ReadySet& decode = ready_d_proc_[static_cast<std::size_t>(remote)];
            const ReadySet& prefill = ready_p_proc_[static_cast<std::size_t>(remote)];
            if (yield_to_decode_[static_cast<std::size_t>(remote)] &&
                !decode.empty()) {
                task = {WorkKind::DECODE, TaskStep::PROC, remote, -1, -1,
                        oldestBatch(decode, best_decode_proc_batch_)};
                found = true;
            } else {
                const Request* overdue = mostOverdue({&decode, &prefill});
                if (overdue != nullptr &&
                    overdue->stage == RequestStage::READY_P_PROC) {
                    int layer_end = system_.num_layers;
                    if (!decode.empty() && overdue->next_prefill_layer == 0 &&
                        system_.num_layers > 1) {
                        layer_end = (system_.num_layers + 1) / 2;
                    }
                    task = {WorkKind::PREFILL, TaskStep::PROC, remote,
                            overdue->next_prefill_layer, layer_end,
                            {overdue->rid}};
                    found = true;
                } else if (!decode.empty()) {
                    if (prefill.empty() &&
                        shouldDeferBatch(
                            decode, best_decode_proc_batch_,
                            RequestStage::WAITING_D_UP, remote,
                            deferred_d_proc_[static_cast<std::size_t>(remote)])) {
                        deferred_d_proc_[static_cast<std::size_t>(remote)] = true;
                    } else {
                        task = {WorkKind::DECODE, TaskStep::PROC, remote, -1,
                                -1,
                                oldestBatch(decode, best_decode_proc_batch_)};
                        found = true;
                    }
                } else if (!prefill.empty()) {
                    const Request* request = requestAt(prefill.begin()->rid);
                    if (!invariant(request != nullptr)) {
                        return assignments;
                    }
                    task = {WorkKind::PREFILL, TaskStep::PROC, remote,
                            request->next_prefill_layer, system_.num_layers,
                            {request->rid}};
                    found = true;
                }
            }

            if (found) {
                Assignment assignment{remote, std::move(task)};
                if (!startAssignment(assignment)) {
                    return assignments;
                }
                yield_to_decode_[static_cast<std::size_t>(remote)] = false;
                deferred_d_proc_[static_cast<std::size_t>(remote)] = false;
                assignments.push_back(std::move(assignment));
            }
        }

        if (!edge_busy_) {
            TaskSpec task;
            const Request* overdue = mostOverdue(
                {&ready_d_post_, &ready_p_post_, &ready_d_pre_, &ready_p_pre_});
            std::optional<RequestStage> selected_stage;
            if (overdue != nullptr) {
                selected_stage = overdue->stage;
            } else if (!ready_d_post_.empty()) {
                selected_stage = RequestStage::READY_D_POST;
            } else if (!ready_p_post_.empty()) {
                selected_stage = RequestStage::READY_P_POST;
            } else if (!ready_d_pre_.empty()) {
                selected_stage = RequestStage::READY_D_PRE;
            } else if (!ready_p_pre_.empty()) {
                selected_stage = RequestStage::READY_P_PRE;
            }

            if (selected_stage == RequestStage::READY_D_POST) {
                if (ready_p_post_.empty() && ready_d_pre_.empty() &&
                    ready_p_pre_.empty() &&
                    shouldDeferBatch(ready_d_post_, best_decode_post_batch_,
                                     RequestStage::WAITING_D_DOWN, -1,
                                     deferred_d_post_)) {
                    deferred_d_post_ = true;
                    selected_stage.reset();
                } else {
                    task = {WorkKind::DECODE, TaskStep::POST, -1, -1, -1,
                            oldestBatch(ready_d_post_,
                                        best_decode_post_batch_)};
                }
            } else if (selected_stage == RequestStage::READY_P_POST) {
                const int rid = ready_p_post_.begin()->rid;
                task = {WorkKind::PREFILL, TaskStep::POST,
                        requestAt(rid)->remote, -1, -1, {rid}};
            } else if (selected_stage == RequestStage::READY_D_PRE) {
                const int transfer_count =
                    std::max(1, ready_d_pre_remote_count_);
                task = {WorkKind::DECODE, TaskStep::PRE, -1, -1, -1,
                        oldestBatch(
                            ready_d_pre_,
                            best_decode_pre_batch_[static_cast<std::size_t>(
                                transfer_count)])};
            } else if (selected_stage == RequestStage::READY_P_PRE) {
                const int rid = ready_p_pre_.begin()->rid;
                Request* request = requestAt(rid);
                if (!invariant(request != nullptr && request->remote == -1)) {
                    return assignments;
                }
                request->remote = leastLoadedRemote();
                next_remote_ = (request->remote + 1) % system_.remote_count;
                task = {WorkKind::PREFILL, TaskStep::PRE, request->remote, -1,
                        -1, {rid}};
            }

            if (selected_stage) {
                Assignment assignment{-1, std::move(task)};
                if (!startAssignment(assignment)) {
                    return assignments;
                }
                deferred_d_post_ = false;
                assignments.push_back(std::move(assignment));
            }
        }

        return assignments;
    }

    void emitAssignments(const std::vector<Assignment>& assignments) {
        output_ << assignments.size() << '\n';
        for (const Assignment& assignment : assignments) {
            if (assignment.server == -1) {
                output_ << 'E';
            } else {
                output_ << 'C' << assignment.server;
            }
            output_ << ' ';
            writeTaskSpec(assignment.task);
            output_ << '\n';
        }
        output_ << std::flush;
    }

private:
    using ReadySet = std::set<ReadyKey, ReadyKeyLess>;
    using TimingCurve = std::vector<std::pair<int, double>>;
    static constexpr int kMaxRequests = 2000;

    static bool invariant(bool condition) {
        assert(condition);
        return condition;
    }

    static double lookupTime(const TimingCurve& curve, int batch_size) {
        const auto upper = std::lower_bound(
            curve.begin(), curve.end(), batch_size,
            [](const auto& point, int size) { return point.first < size; });
        if (upper == curve.begin()) {
            return upper->second;
        }
        if (upper == curve.end()) {
            return curve.back().second;
        }
        if (upper->first == batch_size) {
            return upper->second;
        }

        const auto lower = std::prev(upper);
        const double position = static_cast<double>(batch_size - lower->first) /
                                (upper->first - lower->first);
        return lower->second + position * (upper->second - lower->second);
    }

    double transferServiceTime(int token_count, int transfer_count = 1) const {
        if (transfer_count == 0) {
            return 0.0;
        }
        const long double bytes =
            static_cast<long double>(token_count) * system_.bytes_per_token;
        const long double serialization_ms =
            8.0L * bytes / (system_.bandwidth_gbps * 1'000'000.0L);
        return transfer_count * system_.latency_in_ms +
               static_cast<double>(serialization_ms);
    }

    double transferServiceTimeBytes(std::int64_t size) const {
        const long double serialization_ms =
            8.0L * size / (system_.bandwidth_gbps * 1'000'000.0L);
        return system_.latency_in_ms +
               static_cast<double>(serialization_ms);
    }

    std::vector<int> bestBatchSizes(const TimingCurve& curve,
                                    int transfer_count) const {
        std::vector<int> choices(kMaxRequests + 1, 1);
        long double best_rate = -1.0L;
        int best_size = 1;
        for (int size = 1; size <= kMaxRequests; ++size) {
            const int actual_transfers = std::min(size, transfer_count);
            const long double rate =
                size / (static_cast<long double>(system_.schedule_cost) +
                        lookupTime(curve, size) +
                        transferServiceTime(size, actual_transfers));
            if (rate > best_rate) {
                best_rate = rate;
                best_size = size;
            }
            choices[static_cast<std::size_t>(size)] = best_size;
        }
        return choices;
    }

    bool prepareTimingModel() {
        for (const TaskTimingRow& row : task_times_) {
            if (row.prefill_pre >= 0.0) {
                prefill_pre_curve_.emplace_back(row.batch_size,
                                                row.prefill_pre);
            }
            if (row.prefill_proc >= 0.0) {
                prefill_proc_curve_.emplace_back(row.batch_size,
                                                 row.prefill_proc);
            }
            if (row.prefill_post >= 0.0) {
                prefill_post_curve_.emplace_back(row.batch_size,
                                                 row.prefill_post);
            }
            if (row.decode_pre >= 0.0) {
                decode_pre_curve_.emplace_back(row.batch_size, row.decode_pre);
            }
            if (row.decode_proc >= 0.0) {
                decode_proc_curve_.emplace_back(row.batch_size,
                                                row.decode_proc);
            }
            if (row.decode_post >= 0.0) {
                decode_post_curve_.emplace_back(row.batch_size,
                                                row.decode_post);
            }
        }
        std::array<TimingCurve*, 6> curves = {
            &prefill_pre_curve_, &prefill_proc_curve_, &prefill_post_curve_,
            &decode_pre_curve_,  &decode_proc_curve_,  &decode_post_curve_};
        if (std::any_of(curves.begin(), curves.end(),
                        [](const TimingCurve* curve) { return curve->empty(); })) {
            return false;
        }
        for (TimingCurve* curve : curves) {
            std::sort(curve->begin(), curve->end());
        }

        best_decode_pre_batch_.resize(
            static_cast<std::size_t>(system_.remote_count + 1));
        for (int transfers = 1; transfers <= system_.remote_count; ++transfers) {
            best_decode_pre_batch_[static_cast<std::size_t>(transfers)] =
                bestBatchSizes(decode_pre_curve_, transfers);
        }
        best_decode_proc_batch_ = bestBatchSizes(decode_proc_curve_, 1);
        best_decode_post_batch_ = bestBatchSizes(decode_post_curve_, 0);
        return true;
    }

    std::vector<int> oldestBatch(const ReadySet& ready,
                                 const std::vector<int>& choices) const {
        if (!invariant(!ready.empty() && ready.size() < choices.size())) {
            return {};
        }
        const int count = choices[ready.size()];
        std::vector<int> request_ids;
        request_ids.reserve(static_cast<std::size_t>(count));
        for (auto it = ready.begin();
             it != ready.end() && request_ids.size() < static_cast<std::size_t>(count);
             ++it) {
            request_ids.push_back(it->rid);
        }
        return request_ids;
    }

    int leastLoadedRemote() const {
        int best = next_remote_;
        for (int offset = 1; offset < system_.remote_count; ++offset) {
            const int remote = (next_remote_ + offset) % system_.remote_count;
            if (active_remote_requests_[static_cast<std::size_t>(remote)] <
                active_remote_requests_[static_cast<std::size_t>(best)]) {
                best = remote;
            }
        }
        return best;
    }

    static bool isPrefillStage(RequestStage stage) {
        return stage >= RequestStage::READY_P_PRE &&
               stage <= RequestStage::RUNNING_P_POST;
    }

    static bool isDecodeStage(RequestStage stage) {
        return stage >= RequestStage::READY_D_PRE &&
               stage <= RequestStage::RUNNING_D_POST;
    }

    double taskOccupancy(const TimingCurve& curve, int size) const {
        return system_.schedule_cost + lookupTime(curve, size);
    }

    double normalizedLateness(const Request& request) const {
        const bool prefill = isPrefillStage(request.stage);
        const double target = prefill ? scoring_.slo1 : scoring_.slo2;
        const double reference =
            prefill ? request.arrival_time : request.decode_ready_time;
        return (current_time_ - reference - target) / target;
    }

    const Request* mostOverdue(
        std::initializer_list<const ReadySet*> ready_sets) const {
        const Request* best = nullptr;
        double best_lateness = -1.0;
        for (const ReadySet* ready : ready_sets) {
            if (ready->empty()) {
                continue;
            }
            const Request* request = requestAt(ready->begin()->rid);
            if (!invariant(request != nullptr)) {
                return nullptr;
            }
            const double lateness = normalizedLateness(*request);
            if (lateness < 0.0) {
                continue;
            }
            if (best == nullptr || lateness > best_lateness ||
                (lateness == best_lateness &&
                 std::pair(request->arrival_time, request->rid) <
                     std::pair(best->arrival_time, best->rid))) {
                best = request;
                best_lateness = lateness;
            }
        }
        return best;
    }

    int pendingCount(RequestStage stage, int remote) const {
        if (stage == RequestStage::WAITING_D_UP) {
            if (!invariant(remote >= 0 && remote < system_.remote_count)) {
                return 0;
            }
            return waiting_d_up_[static_cast<std::size_t>(remote)];
        }
        if (!invariant(stage == RequestStage::WAITING_D_DOWN)) {
            return 0;
        }
        return remote < 0
                   ? waiting_d_down_total_
                   : waiting_d_down_[static_cast<std::size_t>(remote)];
    }

    std::optional<double> nextDecodeTransferFinish(Direction direction,
                                                   int remote) const {
        if (direction == Direction::UP) {
            if (!invariant(remote >= 0 && remote < system_.remote_count)) {
                return std::nullopt;
            }
            const std::deque<double>& finishes =
                decode_up_finishes_[static_cast<std::size_t>(remote)];
            return finishes.empty() ? std::nullopt
                                    : std::optional<double>(finishes.front());
        }
        return decode_down_finishes_.empty()
                   ? std::nullopt
                   : std::optional<double>(decode_down_finishes_.front());
    }

    double decodeBatchService(TaskStep step, int size) const {
        if (step == TaskStep::PROC) {
            return taskOccupancy(decode_proc_curve_, size) +
                   transferServiceTime(size);
        }
        if (step == TaskStep::POST) {
            return taskOccupancy(decode_post_curve_, size);
        }
        return taskOccupancy(decode_pre_curve_, size) +
               transferServiceTime(size);
    }

    bool shouldDeferBatch(const ReadySet& ready,
                          const std::vector<int>& batch_choices,
                          RequestStage pending_stage, int remote,
                          bool already_deferred) const {
        if (already_deferred || ready.empty() ||
            scoring_.throughput_weight <= 0.0 ||
            mostOverdue({&ready}) != nullptr) {
            return false;
        }
        const int pending = pendingCount(pending_stage, remote);
        const std::size_t potential_size =
            std::min(batch_choices.size() - 1,
                     ready.size() + static_cast<std::size_t>(pending));
        const int current_batch = batch_choices[ready.size()];
        const int potential_batch = batch_choices[potential_size];
        if (potential_batch <= current_batch) {
            return false;
        }

        const Direction direction = pending_stage == RequestStage::WAITING_D_UP
                                        ? Direction::UP
                                        : Direction::DOWN;
        const std::optional<double> finish =
            nextDecodeTransferFinish(direction, remote);
        if (!invariant(finish.has_value())) {
            return false;
        }
        const TaskStep step = pending_stage == RequestStage::WAITING_D_UP
                                  ? TaskStep::PROC
                                  : TaskStep::POST;
        const double current_rate =
            current_batch / decodeBatchService(step, current_batch);
        const double potential_rate =
            potential_batch / decodeBatchService(step, potential_batch);
        const double rate_gain =
            std::max(0.0, potential_rate / current_rate - 1.0);
        const double wait = std::max(0.0, *finish - current_time_);
        const double throughput_benefit =
            scoring_.throughput_weight * rate_gain;
        const double waiting_penalty =
            scoring_.waiting_weight * wait / scoring_.slo2;
        return throughput_benefit > waiting_penalty;
    }

    static bool atEnd(std::istringstream& input) {
        input >> std::ws;
        return input.eof();
    }

    template <class T>
    static bool parseSingleValue(const std::string& line, T& value) {
        std::istringstream input(line);
        return static_cast<bool>(input >> value) && atEnd(input);
    }

    static bool parseServer(const std::string& token, int& server) {
        if (token == "E") {
            server = -1;
            return true;
        }
        if (token.size() < 2 || token.front() != 'C') {
            return false;
        }
        std::istringstream input(token.substr(1));
        return static_cast<bool>(input >> server) && server >= 0 && atEnd(input);
    }

    static bool parseCompletedTask(std::istringstream& input, TaskSpec& task,
                                   double& duration) {
        std::string work;
        std::string step;
        if (!(input >> work >> step)) {
            return false;
        }

        if (work == "P") {
            task.work = WorkKind::PREFILL;
        } else if (work == "D") {
            task.work = WorkKind::DECODE;
        } else {
            return false;
        }

        if (step == "PRE") {
            task.step = TaskStep::PRE;
        } else if (step == "PROC") {
            task.step = TaskStep::PROC;
        } else if (step == "POST") {
            task.step = TaskStep::POST;
        } else {
            return false;
        }

        if (task.work == WorkKind::PREFILL) {
            int rid = -1;
            if (task.step == TaskStep::PROC) {
                if (!(input >> task.layer_start >> task.layer_end >> task.remote >>
                      rid >> duration)) {
                    return false;
                }
            } else if (!(input >> task.remote >> rid >> duration)) {
                return false;
            }
            task.request_ids.push_back(rid);
        } else {
            int count = 0;
            if (!(input >> task.remote >> count) || count < 1 || count > 2000) {
                return false;
            }
            task.request_ids.resize(static_cast<std::size_t>(count));
            for (int& rid : task.request_ids) {
                if (!(input >> rid)) {
                    return false;
                }
            }
            if (!(input >> duration)) {
                return false;
            }
        }

        return std::isfinite(duration) && atEnd(input);
    }

    std::optional<Event> parseEvent(const std::string& line) const {
        std::istringstream input(line);
        std::string kind;
        if (!(input >> kind)) {
            return std::nullopt;
        }

        Event event;
        if (kind == "ARR") {
            event.kind = EventKind::ARRIVAL;
            if (!(input >> event.rid >> event.input_length) || !atEnd(input)) {
                return std::nullopt;
            }
            return event;
        }

        if (kind == "FIN") {
            event.kind = EventKind::FINISH;
            if (!(input >> event.rid) || !atEnd(input)) {
                return std::nullopt;
            }
            return event;
        }

        if (kind == "TDN") {
            event.kind = EventKind::TASK_DONE;
            std::string server;
            if (!(input >> server) || !parseServer(server, event.server) ||
                !parseCompletedTask(input, event.task, event.duration)) {
                return std::nullopt;
            }
            return event;
        }

        if (kind == "XDN") {
            event.kind = EventKind::TRANSFER_DONE;
            std::string direction;
            std::string transfer_kind;
            int count = 0;
            if (!(input >> direction >> event.remote >> event.size >>
                  transfer_kind >> count) ||
                count < 1 || count > 2000) {
                return std::nullopt;
            }
            if (direction == "UP") {
                event.direction = Direction::UP;
            } else if (direction == "DOWN") {
                event.direction = Direction::DOWN;
            } else {
                return std::nullopt;
            }
            if (transfer_kind == "PRE") {
                event.transfer_kind = TransferKind::PREFILL;
            } else if (transfer_kind == "DEC") {
                event.transfer_kind = TransferKind::DECODE;
            } else {
                return std::nullopt;
            }
            event.request_ids.resize(static_cast<std::size_t>(count));
            for (int& rid : event.request_ids) {
                if (!(input >> rid)) {
                    return std::nullopt;
                }
            }
            if (!atEnd(input)) {
                return std::nullopt;
            }
            return event;
        }

        return std::nullopt;
    }

    Request* requestAt(int rid) {
        if (rid < 0 || static_cast<std::size_t>(rid) >= requests_.size() ||
            !requests_[static_cast<std::size_t>(rid)]) {
            return nullptr;
        }
        return &*requests_[static_cast<std::size_t>(rid)];
    }

    const Request* requestAt(int rid) const {
        if (rid < 0 || static_cast<std::size_t>(rid) >= requests_.size() ||
            !requests_[static_cast<std::size_t>(rid)]) {
            return nullptr;
        }
        return &*requests_[static_cast<std::size_t>(rid)];
    }

    static ReadyKey readyKey(const Request& request) {
        const double priority_time = isPrefillStage(request.stage)
                                         ? request.arrival_time
                                         : request.decode_ready_time;
        return {priority_time, request.rid};
    }

    void adjustStageCounters(const Request& request, RequestStage stage,
                             int delta) {
        if (isDecodeStage(stage)) {
            const std::size_t remote = static_cast<std::size_t>(request.remote);
            decode_active_by_remote_[remote] += delta;
            total_decode_active_ += delta;
            static_cast<void>(invariant(decode_active_by_remote_[remote] >= 0 &&
                                        total_decode_active_ >= 0));
        }
        if (stage == RequestStage::WAITING_D_UP) {
            waiting_d_up_[static_cast<std::size_t>(request.remote)] += delta;
            static_cast<void>(invariant(
                waiting_d_up_[static_cast<std::size_t>(request.remote)] >= 0));
        } else if (stage == RequestStage::WAITING_D_DOWN) {
            waiting_d_down_[static_cast<std::size_t>(request.remote)] += delta;
            waiting_d_down_total_ += delta;
            static_cast<void>(invariant(
                waiting_d_down_[static_cast<std::size_t>(request.remote)] >= 0 &&
                waiting_d_down_total_ >= 0));
        } else if (stage == RequestStage::READY_D_PRE) {
            int& count =
                ready_d_pre_by_remote_[static_cast<std::size_t>(request.remote)];
            if (delta > 0 && count == 0) {
                ++ready_d_pre_remote_count_;
            }
            count += delta;
            if (delta < 0 && count == 0) {
                --ready_d_pre_remote_count_;
            }
            static_cast<void>(invariant(count >= 0 &&
                                        ready_d_pre_remote_count_ >= 0));
        }
    }

    void addReady(const Request& request) {
        bool inserted = false;
        const ReadyKey key = readyKey(request);
        switch (request.stage) {
            case RequestStage::READY_P_PRE:
                inserted = ready_p_pre_.insert(key).second;
                break;
            case RequestStage::READY_P_PROC:
                inserted = ready_p_proc_[static_cast<std::size_t>(request.remote)]
                               .insert(key)
                               .second;
                break;
            case RequestStage::READY_P_POST:
                inserted = ready_p_post_.insert(key).second;
                break;
            case RequestStage::READY_D_PRE:
                inserted = ready_d_pre_.insert(key).second;
                break;
            case RequestStage::READY_D_PROC:
                inserted = ready_d_proc_[static_cast<std::size_t>(request.remote)]
                               .insert(key)
                               .second;
                break;
            case RequestStage::READY_D_POST:
                inserted = ready_d_post_.insert(key).second;
                break;
            default:
                return;
        }
        static_cast<void>(invariant(inserted));
    }

    void removeReady(const Request& request) {
        std::size_t erased = 0;
        const ReadyKey key = readyKey(request);
        switch (request.stage) {
            case RequestStage::READY_P_PRE:
                erased = ready_p_pre_.erase(key);
                break;
            case RequestStage::READY_P_PROC:
                erased = ready_p_proc_[static_cast<std::size_t>(request.remote)]
                             .erase(key);
                break;
            case RequestStage::READY_P_POST:
                erased = ready_p_post_.erase(key);
                break;
            case RequestStage::READY_D_PRE:
                erased = ready_d_pre_.erase(key);
                break;
            case RequestStage::READY_D_PROC:
                erased = ready_d_proc_[static_cast<std::size_t>(request.remote)]
                             .erase(key);
                break;
            case RequestStage::READY_D_POST:
                erased = ready_d_post_.erase(key);
                break;
            default:
                return;
        }
        static_cast<void>(invariant(erased == 1));
    }

    void setStage(Request& request, RequestStage stage) {
        removeReady(request);
        adjustStageCounters(request, request.stage, -1);
        request.stage = stage;
        adjustStageCounters(request, request.stage, 1);
        addReady(request);
    }

    static RequestStage readyStageFor(const TaskSpec& task) {
        if (task.work == WorkKind::PREFILL) {
            if (task.step == TaskStep::PRE) {
                return RequestStage::READY_P_PRE;
            }
            if (task.step == TaskStep::PROC) {
                return RequestStage::READY_P_PROC;
            }
            return RequestStage::READY_P_POST;
        }
        if (task.step == TaskStep::PRE) {
            return RequestStage::READY_D_PRE;
        }
        if (task.step == TaskStep::PROC) {
            return RequestStage::READY_D_PROC;
        }
        return RequestStage::READY_D_POST;
    }

    static RequestStage runningStageFor(const TaskSpec& task) {
        if (task.work == WorkKind::PREFILL) {
            if (task.step == TaskStep::PRE) {
                return RequestStage::RUNNING_P_PRE;
            }
            if (task.step == TaskStep::PROC) {
                return RequestStage::RUNNING_P_PROC;
            }
            return RequestStage::RUNNING_P_POST;
        }
        if (task.step == TaskStep::PRE) {
            return RequestStage::RUNNING_D_PRE;
        }
        if (task.step == TaskStep::PROC) {
            return RequestStage::RUNNING_D_PROC;
        }
        return RequestStage::RUNNING_D_POST;
    }

    RequestStage completedStageFor(const TaskSpec& task) const {
        if (task.work == WorkKind::PREFILL) {
            if (task.step == TaskStep::PRE) {
                return RequestStage::WAITING_P_UP;
            }
            if (task.step == TaskStep::PROC) {
                return task.layer_end == system_.num_layers
                           ? RequestStage::WAITING_P_DOWN
                           : RequestStage::READY_P_PROC;
            }
            return RequestStage::READY_D_PRE;
        }
        if (task.step == TaskStep::PRE) {
            return RequestStage::WAITING_D_UP;
        }
        if (task.step == TaskStep::PROC) {
            return RequestStage::WAITING_D_DOWN;
        }
        return RequestStage::READY_D_PRE;
    }

    int expectedServer(const TaskSpec& task) const {
        return task.step == TaskStep::PROC ? task.remote : -1;
    }

    bool validateTaskShape(const TaskSpec& task) const {
        if (!invariant(!task.request_ids.empty())) {
            return false;
        }
        std::set<int> unique_ids(task.request_ids.begin(), task.request_ids.end());
        if (!invariant(unique_ids.size() == task.request_ids.size())) {
            return false;
        }

        const bool valid_remote =
            task.remote >= 0 && task.remote < system_.remote_count;
        if (task.work == WorkKind::PREFILL) {
            if (!invariant(task.request_ids.size() == 1 && valid_remote)) {
                return false;
            }
            if (task.step == TaskStep::PROC) {
                return invariant(task.layer_start >= 0 &&
                                 task.layer_start < task.layer_end &&
                                 task.layer_end <= system_.num_layers);
            }
            return invariant(task.layer_start == -1 && task.layer_end == -1);
        }

        if (!invariant(task.layer_start == -1 && task.layer_end == -1)) {
            return false;
        }
        if (task.step == TaskStep::PROC) {
            return invariant(valid_remote);
        }
        return invariant(task.remote == -1);
    }

    bool validateRequests(const TaskSpec& task, RequestStage expected) const {
        for (int rid : task.request_ids) {
            const Request* request = requestAt(rid);
            if (!invariant(request != nullptr && !request->finished &&
                           request->stage == expected)) {
                return false;
            }
            if ((task.work == WorkKind::PREFILL ||
                 task.step == TaskStep::PROC) &&
                !invariant(request->remote == task.remote)) {
                return false;
            }
            if (task.work == WorkKind::PREFILL &&
                task.step == TaskStep::PROC &&
                !invariant(request->next_prefill_layer == task.layer_start)) {
                return false;
            }
        }
        return true;
    }

    bool startAssignment(const Assignment& assignment) {
        if (!validateTaskShape(assignment.task) ||
            !invariant(assignment.server == expectedServer(assignment.task)) ||
            !validateRequests(assignment.task, readyStageFor(assignment.task))) {
            return false;
        }

        if (assignment.server == -1) {
            if (!invariant(!edge_busy_ && !edge_task_)) {
                return false;
            }
        } else {
            const std::size_t remote = static_cast<std::size_t>(assignment.server);
            if (!invariant(!remote_busy_[remote] && !remote_tasks_[remote])) {
                return false;
            }
        }

        if (assignment.server == -1) {
            edge_busy_ = true;
            edge_task_ = assignment.task;
        } else {
            const std::size_t remote = static_cast<std::size_t>(assignment.server);
            remote_busy_[remote] = true;
            remote_tasks_[remote] = assignment.task;
        }
        if (assignment.task.work == WorkKind::PREFILL &&
            assignment.task.step == TaskStep::PRE) {
            ++active_remote_requests_[
                static_cast<std::size_t>(assignment.task.remote)];
        }
        for (int rid : assignment.task.request_ids) {
            setStage(*requestAt(rid), runningStageFor(assignment.task));
        }
        return true;
    }

    bool applyEvent(const Event& event, double timestamp) {
        if (event.kind == EventKind::ARRIVAL) {
            return applyArrival(event, timestamp);
        }
        if (event.kind == EventKind::TASK_DONE) {
            return applyTaskDone(event, timestamp);
        }
        if (event.kind == EventKind::TRANSFER_DONE) {
            return applyTransferDone(event);
        }
        return false;
    }

    bool applyArrival(const Event& event, double timestamp) {
        if (!invariant(event.rid >= 0 && event.input_length > 0)) {
            return false;
        }
        const std::size_t rid = static_cast<std::size_t>(event.rid);
        if (requests_.size() <= rid) {
            requests_.resize(rid + 1);
        }
        if (!invariant(!requests_[rid])) {
            return false;
        }
        requests_[rid] = Request{event.rid, event.input_length, -1,
                                 RequestStage::READY_P_PRE, timestamp, timestamp,
                                 false};
        addReady(*requests_[rid]);
        return true;
    }

    void enqueueTransfer(Direction direction, TransferKind kind, int remote,
                         std::int64_t size, std::vector<int> request_ids,
                         double timestamp) {
        std::deque<QueuedTransfer>& queue =
            direction == Direction::UP ? up_transfers_ : down_transfers_;
        double& tail =
            direction == Direction::UP ? up_transfer_tail_ : down_transfer_tail_;
        const double start = queue.empty() ? timestamp : std::max(timestamp, tail);
        tail = start + transferServiceTimeBytes(size);
        queue.push_back(
            {kind, remote, size, std::move(request_ids), tail});
        if (kind == TransferKind::DECODE) {
            if (direction == Direction::UP) {
                decode_up_finishes_[static_cast<std::size_t>(remote)].push_back(
                    tail);
            } else {
                decode_down_finishes_.push_back(tail);
            }
        }
    }

    bool enqueueTransfersForTask(const TaskSpec& task, double timestamp) {
        if (task.work == WorkKind::PREFILL) {
            if (task.step == TaskStep::POST ||
                (task.step == TaskStep::PROC &&
                 task.layer_end < system_.num_layers)) {
                return true;
            }
            const Request* request = requestAt(task.request_ids.front());
            if (!invariant(request != nullptr)) {
                return false;
            }
            const Direction direction = task.step == TaskStep::PRE
                                            ? Direction::UP
                                            : Direction::DOWN;
            const std::int64_t size =
                static_cast<std::int64_t>(request->input_length) *
                system_.bytes_per_token;
            enqueueTransfer(direction, TransferKind::PREFILL, task.remote, size,
                            task.request_ids, timestamp);
            return true;
        }

        if (task.step == TaskStep::POST) {
            return true;
        }
        if (task.step == TaskStep::PROC) {
            const std::int64_t size =
                static_cast<std::int64_t>(task.request_ids.size()) *
                system_.bytes_per_token;
            enqueueTransfer(Direction::DOWN, TransferKind::DECODE, task.remote,
                            size, task.request_ids, timestamp);
            return true;
        }

        std::vector<std::vector<int>> by_remote(
            static_cast<std::size_t>(system_.remote_count));
        for (int rid : task.request_ids) {
            const Request* request = requestAt(rid);
            if (!invariant(request != nullptr)) {
                return false;
            }
            by_remote[static_cast<std::size_t>(request->remote)].push_back(rid);
        }
        for (int remote = 0; remote < system_.remote_count; ++remote) {
            std::vector<int>& ids = by_remote[static_cast<std::size_t>(remote)];
            if (ids.empty()) {
                continue;
            }
            const std::int64_t size =
                static_cast<std::int64_t>(ids.size()) * system_.bytes_per_token;
            enqueueTransfer(Direction::UP, TransferKind::DECODE, remote, size,
                            std::move(ids), timestamp);
        }
        return true;
    }

    bool consumeTransfer(const Event& event) {
        std::deque<QueuedTransfer>& queue = event.direction == Direction::UP
                                                   ? up_transfers_
                                                   : down_transfers_;
        if (!invariant(!queue.empty())) {
            return false;
        }
        const QueuedTransfer& expected = queue.front();
        if (!invariant(expected.kind == event.transfer_kind &&
                       expected.remote == event.remote &&
                       expected.size == event.size &&
                       expected.request_ids == event.request_ids)) {
            return false;
        }
        if (expected.kind == TransferKind::DECODE) {
            std::deque<double>& finishes =
                event.direction == Direction::UP
                    ? decode_up_finishes_[static_cast<std::size_t>(event.remote)]
                    : decode_down_finishes_;
            if (!invariant(!finishes.empty() &&
                           finishes.front() == expected.expected_finish_time)) {
                return false;
            }
            finishes.pop_front();
        }
        queue.pop_front();
        return true;
    }

    bool applyTaskDone(const Event& event, double timestamp) {
        if (!validateTaskShape(event.task) ||
            !invariant(event.server == expectedServer(event.task)) ||
            !validateRequests(event.task, runningStageFor(event.task))) {
            return false;
        }

        if (event.server == -1) {
            if (!invariant(edge_busy_ && edge_task_ &&
                           *edge_task_ == event.task)) {
                return false;
            }
            edge_busy_ = false;
            edge_task_.reset();
        } else {
            const std::size_t remote = static_cast<std::size_t>(event.server);
            if (!invariant(remote < remote_busy_.size() && remote_busy_[remote] &&
                           remote_tasks_[remote] &&
                           *remote_tasks_[remote] == event.task)) {
                return false;
            }
            remote_busy_[remote] = false;
            remote_tasks_[remote].reset();
        }

        if (!enqueueTransfersForTask(event.task, timestamp)) {
            return false;
        }

        const RequestStage next = completedStageFor(event.task);
        for (int rid : event.task.request_ids) {
            Request* request = requestAt(rid);
            if (event.task.work == WorkKind::PREFILL &&
                event.task.step == TaskStep::PROC) {
                request->next_prefill_layer = event.task.layer_end;
                if (event.task.layer_end < system_.num_layers) {
                    yield_to_decode_[static_cast<std::size_t>(event.task.remote)] =
                        true;
                }
            }
            if (event.task.step == TaskStep::POST) {
                request->decode_ready_time = timestamp;
            }
            setStage(*request, next);
        }
        return true;
    }

    bool applyTransferDone(const Event& event) {
        if (!invariant(event.remote >= 0 &&
                       event.remote < system_.remote_count && event.size >= 0)) {
            return false;
        }

        std::set<int> unique_ids(event.request_ids.begin(),
                                 event.request_ids.end());
        if (!invariant(!event.request_ids.empty() &&
                       unique_ids.size() == event.request_ids.size())) {
            return false;
        }

        RequestStage expected;
        RequestStage next;
        std::int64_t expected_size = 0;
        if (event.transfer_kind == TransferKind::PREFILL) {
            if (!invariant(event.request_ids.size() == 1)) {
                return false;
            }
            const Request* request = requestAt(event.request_ids.front());
            if (!invariant(request != nullptr)) {
                return false;
            }
            expected_size = static_cast<std::int64_t>(request->input_length) *
                            system_.bytes_per_token;
            if (event.direction == Direction::UP) {
                expected = RequestStage::WAITING_P_UP;
                next = RequestStage::READY_P_PROC;
            } else {
                expected = RequestStage::WAITING_P_DOWN;
                next = RequestStage::READY_P_POST;
            }
        } else {
            expected_size = static_cast<std::int64_t>(event.request_ids.size()) *
                            system_.bytes_per_token;
            if (event.direction == Direction::UP) {
                expected = RequestStage::WAITING_D_UP;
                next = RequestStage::READY_D_PROC;
            } else {
                expected = RequestStage::WAITING_D_DOWN;
                next = RequestStage::READY_D_POST;
            }
        }

        if (!invariant(event.size == expected_size)) {
            return false;
        }
        for (int rid : event.request_ids) {
            const Request* request = requestAt(rid);
            if (!invariant(request != nullptr && !request->finished &&
                           request->remote == event.remote &&
                           request->stage == expected)) {
                return false;
            }
        }
        if (!consumeTransfer(event)) {
            return false;
        }
        for (int rid : event.request_ids) {
            setStage(*requestAt(rid), next);
        }
        return true;
    }

    bool applyFinish(int rid) {
        Request* request = requestAt(rid);
        if (!invariant(request != nullptr && !request->finished &&
                       request->stage == RequestStage::READY_D_PRE)) {
            return false;
        }
        setStage(*request, RequestStage::FINISHED);
        request->finished = true;
        int& active =
            active_remote_requests_[static_cast<std::size_t>(request->remote)];
        --active;
        static_cast<void>(invariant(active >= 0));
        return true;
    }

    void writeTaskSpec(const TaskSpec& task) {
        output_ << (task.work == WorkKind::PREFILL ? 'P' : 'D') << ' ';
        if (task.step == TaskStep::PRE) {
            output_ << "PRE";
        } else if (task.step == TaskStep::PROC) {
            output_ << "PROC";
        } else {
            output_ << "POST";
        }

        if (task.work == WorkKind::PREFILL && task.step == TaskStep::PROC) {
            output_ << ' ' << task.layer_start << ' ' << task.layer_end;
        }
        output_ << ' ' << task.remote;
        if (task.work == WorkKind::DECODE) {
            output_ << ' ' << task.request_ids.size();
        }
        for (int rid : task.request_ids) {
            output_ << ' ' << rid;
        }
    }

    std::istream& input_;
    std::ostream& output_;
    SystemConfig system_;
    ScoringConfig scoring_;
    std::vector<TaskTimingRow> task_times_;
    TimingCurve prefill_pre_curve_;
    TimingCurve prefill_proc_curve_;
    TimingCurve prefill_post_curve_;
    TimingCurve decode_pre_curve_;
    TimingCurve decode_proc_curve_;
    TimingCurve decode_post_curve_;
    std::vector<std::vector<int>> best_decode_pre_batch_;
    std::vector<int> best_decode_proc_batch_;
    std::vector<int> best_decode_post_batch_;

    std::vector<std::optional<Request>> requests_;
    double current_time_ = 0.0;
    bool edge_busy_ = false;
    std::vector<bool> remote_busy_;
    std::vector<bool> yield_to_decode_;
    std::vector<bool> deferred_d_proc_;
    bool deferred_d_post_ = false;
    std::vector<int> active_remote_requests_;
    std::vector<int> decode_active_by_remote_;
    int total_decode_active_ = 0;
    std::vector<int> waiting_d_up_;
    std::vector<int> waiting_d_down_;
    int waiting_d_down_total_ = 0;
    std::vector<int> ready_d_pre_by_remote_;
    int ready_d_pre_remote_count_ = 0;
    std::optional<TaskSpec> edge_task_;
    std::vector<std::optional<TaskSpec>> remote_tasks_;
    int next_remote_ = 0;

    std::deque<QueuedTransfer> up_transfers_;
    std::deque<QueuedTransfer> down_transfers_;
    std::vector<std::deque<double>> decode_up_finishes_;
    std::deque<double> decode_down_finishes_;
    double up_transfer_tail_ = 0.0;
    double down_transfer_tail_ = 0.0;

    ReadySet ready_p_pre_;
    ReadySet ready_p_post_;
    ReadySet ready_d_pre_;
    ReadySet ready_d_post_;
    std::vector<ReadySet> ready_p_proc_;
    std::vector<ReadySet> ready_d_proc_;
};

int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    Scheduler scheduler(std::cin, std::cout);
    if (!scheduler.readStartup()) {
        return 0;
    }
    while (scheduler.readAndProcessFrame()) {
    }
    return 0;
}
