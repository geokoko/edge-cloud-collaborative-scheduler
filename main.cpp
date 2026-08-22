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

struct Request {
    int rid = -1;
    int input_length = 0;
    int remote = -1;
    RequestStage stage = RequestStage::READY_P_PRE;
    double arrival_time = 0.0;
    double decode_ready_time = 0.0;
    bool finished = false;
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
            system_.bytes_per_token <= 0 || scoring_.slo1 <= 0.0 ||
            scoring_.slo2 <= 0.0) {
            return false;
        }
        if (!prepareDecodeBatching()) {
            return false;
        }
        remote_busy_.assign(static_cast<std::size_t>(system_.remote_count), false);
        remote_tasks_.resize(static_cast<std::size_t>(system_.remote_count));
        ready_p_proc_.resize(static_cast<std::size_t>(system_.remote_count));
        ready_d_proc_.resize(static_cast<std::size_t>(system_.remote_count));
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
            const Request* overdue = mostOverdue({&decode, &prefill});
            if (overdue != nullptr &&
                overdue->stage == RequestStage::READY_P_PROC) {
                task = {WorkKind::PREFILL, TaskStep::PROC, remote, 0,
                        system_.num_layers, {overdue->rid}};
                found = true;
            } else if (!decode.empty()) {
                const std::vector<int>& batch_sizes =
                    overdue != nullptr ? fastest_decode_proc_batch_
                                       : best_decode_proc_batch_;
                task = {WorkKind::DECODE, TaskStep::PROC, remote, -1, -1,
                        oldestBatch(decode, batch_sizes)};
                found = true;
            } else if (!prefill.empty()) {
                task = {WorkKind::PREFILL, TaskStep::PROC, remote, 0,
                        system_.num_layers, {prefill.begin()->rid}};
                found = true;
            }

            if (found) {
                Assignment assignment{remote, std::move(task)};
                if (!startAssignment(assignment)) {
                    return assignments;
                }
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
                const std::vector<int>& batch_sizes =
                    overdue != nullptr ? fastest_decode_post_batch_
                                       : best_decode_post_batch_;
                task = {WorkKind::DECODE, TaskStep::POST, -1, -1, -1,
                        oldestBatch(ready_d_post_, batch_sizes)};
            } else if (selected_stage == RequestStage::READY_P_POST) {
                const int rid = ready_p_post_.begin()->rid;
                task = {WorkKind::PREFILL, TaskStep::POST,
                        requestAt(rid)->remote, -1, -1, {rid}};
            } else if (selected_stage == RequestStage::READY_D_PRE) {
                const std::vector<int>& batch_sizes =
                    overdue != nullptr ? fastest_decode_pre_batch_
                                       : best_decode_pre_batch_;
                task = {WorkKind::DECODE, TaskStep::PRE, -1, -1, -1,
                        oldestBatch(ready_d_pre_, batch_sizes)};
            } else if (selected_stage == RequestStage::READY_P_PRE) {
                const int rid = ready_p_pre_.begin()->rid;
                Request* request = requestAt(rid);
                if (!invariant(request != nullptr && request->remote == -1)) {
                    return assignments;
                }
                request->remote = next_remote_;
                next_remote_ = (next_remote_ + 1) % system_.remote_count;
                task = {WorkKind::PREFILL, TaskStep::PRE, request->remote, -1,
                        -1, {rid}};
            }

            if (selected_stage) {
                Assignment assignment{-1, std::move(task)};
                if (!startAssignment(assignment)) {
                    return assignments;
                }
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

    std::vector<int> bestBatchSizes(const TimingCurve& curve) const {
        std::vector<int> choices(kMaxRequests + 1, 1);
        long double best_rate = -1.0L;
        int best_size = 1;
        for (int size = 1; size <= kMaxRequests; ++size) {
            // ponytail: current-ready compute throughput only; add transfer-aware
            // planning only after this heuristic is measured on judge data.
            const long double rate =
                size / (static_cast<long double>(system_.schedule_cost) +
                        lookupTime(curve, size));
            if (rate > best_rate) {
                best_rate = rate;
                best_size = size;
            }
            choices[static_cast<std::size_t>(size)] = best_size;
        }
        return choices;
    }

    static std::vector<int> fastestBatchSizes(const TimingCurve& curve) {
        std::vector<int> choices(kMaxRequests + 1, 1);
        double best_time = std::numeric_limits<double>::infinity();
        int best_size = 1;
        for (int size = 1; size <= kMaxRequests; ++size) {
            const double time = lookupTime(curve, size);
            if (time < best_time) {
                best_time = time;
                best_size = size;
            }
            choices[static_cast<std::size_t>(size)] = best_size;
        }
        return choices;
    }

    bool prepareDecodeBatching() {
        TimingCurve decode_pre;
        TimingCurve decode_proc;
        TimingCurve decode_post;
        for (const TaskTimingRow& row : task_times_) {
            if (row.decode_pre >= 0.0) {
                decode_pre.emplace_back(row.batch_size, row.decode_pre);
            }
            if (row.decode_proc >= 0.0) {
                decode_proc.emplace_back(row.batch_size, row.decode_proc);
            }
            if (row.decode_post >= 0.0) {
                decode_post.emplace_back(row.batch_size, row.decode_post);
            }
        }
        if (decode_pre.empty() || decode_proc.empty() || decode_post.empty()) {
            return false;
        }
        std::sort(decode_pre.begin(), decode_pre.end());
        std::sort(decode_proc.begin(), decode_proc.end());
        std::sort(decode_post.begin(), decode_post.end());
        best_decode_pre_batch_ = bestBatchSizes(decode_pre);
        best_decode_proc_batch_ = bestBatchSizes(decode_proc);
        best_decode_post_batch_ = bestBatchSizes(decode_post);
        fastest_decode_pre_batch_ = fastestBatchSizes(decode_pre);
        fastest_decode_proc_batch_ = fastestBatchSizes(decode_proc);
        fastest_decode_post_batch_ = fastestBatchSizes(decode_post);
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

    static bool isPrefillStage(RequestStage stage) {
        return stage >= RequestStage::READY_P_PRE &&
               stage <= RequestStage::RUNNING_P_POST;
    }

    double normalizedLateness(const Request& request) const {
        // ponytail: hard end-to-end deadlines only; add remaining-work slack
        // estimates only if judge measurements justify prediction complexity.
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
        request.stage = stage;
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

    static RequestStage completedStageFor(const TaskSpec& task) {
        if (task.work == WorkKind::PREFILL) {
            if (task.step == TaskStep::PRE) {
                return RequestStage::WAITING_P_UP;
            }
            if (task.step == TaskStep::PROC) {
                return RequestStage::WAITING_P_DOWN;
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
                return invariant(task.layer_start == 0 &&
                                 task.layer_end == system_.num_layers);
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

        const RequestStage next = completedStageFor(event.task);
        for (int rid : event.task.request_ids) {
            Request* request = requestAt(rid);
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
    std::vector<int> best_decode_pre_batch_;
    std::vector<int> best_decode_proc_batch_;
    std::vector<int> best_decode_post_batch_;
    std::vector<int> fastest_decode_pre_batch_;
    std::vector<int> fastest_decode_proc_batch_;
    std::vector<int> fastest_decode_post_batch_;

    std::vector<std::optional<Request>> requests_;
    double current_time_ = 0.0;
    bool edge_busy_ = false;
    std::vector<bool> remote_busy_;
    std::optional<TaskSpec> edge_task_;
    std::vector<std::optional<TaskSpec>> remote_tasks_;
    int next_remote_ = 0;

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
