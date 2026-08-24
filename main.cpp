#include <bits/stdc++.h>
#include <cassert>

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

static_assert(
  static_cast<int>(RequestStage::RUNNING_P_PRE) ==
    static_cast<int>(RequestStage::READY_P_PRE) + 1 &&
  static_cast<int>(RequestStage::RUNNING_P_PROC) ==
    static_cast<int>(RequestStage::READY_P_PROC) + 1 &&
  static_cast<int>(RequestStage::RUNNING_P_POST) ==
    static_cast<int>(RequestStage::READY_P_POST) + 1 &&
  static_cast<int>(RequestStage::RUNNING_D_PRE) ==
    static_cast<int>(RequestStage::READY_D_PRE) + 1 &&
  static_cast<int>(RequestStage::RUNNING_D_PROC) ==
    static_cast<int>(RequestStage::READY_D_PROC) + 1 &&
  static_cast<int>(RequestStage::RUNNING_D_POST) ==
    static_cast<int>(RequestStage::READY_D_POST) + 1);

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
  bool has_produced_token = false;
  int next_prefill_layer = 0;
  double transfer_ready_time = 0.0;
  int decode_wave = -1;
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

struct ReadyKey {
  bool first_token_decode = false;
  double priority_time = 0.0;
  int rid = -1;

  auto operator<=>(const ReadyKey&) const = default;
};

struct PipelinePlan {
  int batch_size = 1;
  int active_remotes = 1;
  long double throughput = 0.0L;
};

struct DecodeWave {
  std::vector<int> request_ids;
  int ready_post = 0;
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
    for (int row = 0; row < row_count; ++row) {
      int batch_size = 0;
      double prefill_pre = 0.0;
      double prefill_proc = 0.0;
      double prefill_post = 0.0;
      double decode_pre = 0.0;
      double decode_proc = 0.0;
      double decode_post = 0.0;
      if (!(input_ >> batch_size >> prefill_pre >> prefill_proc >>
         prefill_post >> decode_pre >> decode_proc >> decode_post)) {
        return false;
      }
      if (prefill_pre >= 0.0) {
        prefill_pre_.emplace_back(batch_size, prefill_pre);
      }
      if (prefill_proc >= 0.0) {
        prefill_proc_.emplace_back(batch_size, prefill_proc);
      }
      if (prefill_post >= 0.0) {
        prefill_post_.emplace_back(batch_size, prefill_post);
      }
      if (decode_pre >= 0.0) {
        decode_pre_.emplace_back(batch_size, decode_pre);
      }
      if (decode_proc >= 0.0) {
        decode_proc_.emplace_back(batch_size, decode_proc);
      }
      if (decode_post >= 0.0) {
        decode_post_.emplace_back(batch_size, decode_post);
      }
    }

    if (system_.remote_count <= 0 || system_.num_layers <= 0 ||
      system_.bytes_per_token <= 0 || scoring_.slo1 <= 0.0 ||
      scoring_.slo2 <= 0.0) {
      return false;
    }
    if (!prepareSchedulingData()) {
      return false;
    }
    yield_to_decode_.assign(static_cast<std::size_t>(system_.remote_count),
                false);
    deferred_d_proc_.assign(static_cast<std::size_t>(system_.remote_count),
                false);
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
      if (remote_tasks_[static_cast<std::size_t>(remote)]) {
        continue;
      }

      TaskSpec task;
      bool found = false;
      const ReadySet& decode = ready_d_proc_[static_cast<std::size_t>(remote)];
      const ReadySet& prefill = ready_p_proc_[static_cast<std::size_t>(remote)];
      const bool active_decode = frontHasProducedToken(decode);
      if (yield_to_decode_[static_cast<std::size_t>(remote)] &&
        active_decode) {
        task = {WorkKind::DECODE, TaskStep::PROC, remote, -1, -1,
            decodeProcBatch(decode)};
        found = true;
      } else {
        const Request* overdue = mostOverdue({&decode, &prefill});
        if (overdue != nullptr &&
          overdue->stage == RequestStage::READY_P_PROC) {
          int layer_end = system_.num_layers;
          if (active_decode && overdue->next_prefill_layer == 0 &&
            system_.num_layers > 1) {
            layer_end = (system_.num_layers + 1) / 2;
          }
          task = {WorkKind::PREFILL, TaskStep::PROC, remote,
              overdue->next_prefill_layer, layer_end,
              {overdue->rid}};
          found = true;
        } else if (!decode.empty() &&
              (active_decode || prefill.empty())) {
          if (!usesJointDecodePlan() && prefill.empty() &&
            shouldDeferBatch(
              decode, best_decode_proc_batch_, decode_proc_,
              RequestStage::WAITING_D_UP, remote,
              deferred_d_proc_[static_cast<std::size_t>(remote)])) {
            deferred_d_proc_[static_cast<std::size_t>(remote)] = true;
          } else {
            task = {WorkKind::DECODE, TaskStep::PROC, remote, -1,
                -1, decodeProcBatch(decode)};
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

    if (!edge_task_) {
      TaskSpec task;
      const Request* overdue = mostOverdue(
        {&ready_d_post_, &ready_p_post_, &ready_d_pre_, &ready_p_pre_});
      std::vector<int> decode_post_batch;
      if (usesJointDecodePlan()) {
        if (overdue != nullptr &&
          overdue->stage == RequestStage::READY_D_POST) {
          const auto wave = decode_waves_.find(overdue->decode_wave);
          if (wave != decode_waves_.end() &&
            complete_decode_waves_.contains(wave->first)) {
            decode_post_batch = wave->second.request_ids;
          } else {
            decode_post_batch = {overdue->rid};
          }
        } else {
          decode_post_batch = completeDecodePostBatch();
        }
      }
      const bool decode_post_available =
        usesJointDecodePlan() ? !decode_post_batch.empty()
                   : !ready_d_post_.empty();
      const bool active_d_post =
        usesJointDecodePlan()
          ? batchHasProducedToken(decode_post_batch)
          : frontHasProducedToken(ready_d_post_);
      const bool active_d_pre = frontHasProducedToken(ready_d_pre_);
      std::optional<RequestStage> selected_stage;
      if (overdue != nullptr) {
        selected_stage = overdue->stage;
      } else if (active_d_post) {
        selected_stage = RequestStage::READY_D_POST;
      } else if (active_d_pre) {
        selected_stage = RequestStage::READY_D_PRE;
      } else if (!ready_p_post_.empty()) {
        selected_stage = RequestStage::READY_P_POST;
      } else if (!ready_p_pre_.empty()) {
        selected_stage = RequestStage::READY_P_PRE;
      } else if (decode_post_available) {
        selected_stage = RequestStage::READY_D_POST;
      } else if (!ready_d_pre_.empty()) {
        selected_stage = RequestStage::READY_D_PRE;
      }

      if (selected_stage == RequestStage::READY_D_POST) {
        if (usesJointDecodePlan()) {
          if (!invariant(!decode_post_batch.empty())) {
            return assignments;
          }
          task = {WorkKind::DECODE, TaskStep::POST, -1, -1, -1,
              std::move(decode_post_batch)};
        } else if (ready_p_post_.empty() && ready_d_pre_.empty() &&
          ready_p_pre_.empty() &&
          shouldDeferBatch(ready_d_post_, best_decode_post_batch_,
                   decode_post_,
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
        if (shouldAccumulateFirstDecodeWave()) {
          selected_stage.reset();
        } else {
          task = {WorkKind::DECODE, TaskStep::PRE, -1, -1, -1,
              decodePreBatch()};
        }
      } else if (selected_stage == RequestStage::READY_P_PRE) {
        const Request* candidate =
          overdue != nullptr &&
              overdue->stage == RequestStage::READY_P_PRE &&
              normalizedLateness(*overdue) >=
                std::max(1.0, scoring_.distance_base)
            ? overdue
            : shortestPrefill(ready_p_pre_);
        if (!invariant(candidate != nullptr)) {
          return assignments;
        }
        const int rid = candidate->rid;
        Request* request = requestAt(rid);
        if (!invariant(request != nullptr && request->remote == -1)) {
          return assignments;
        }
        if (shouldDeferPrefill(*request)) {
          selected_stage.reset();
        } else {
          const int remote_limit = placementRemoteLimit();
          request->remote = leastLoadedRemote(remote_limit);
          next_remote_ = (request->remote + 1) % remote_limit;
          task = {WorkKind::PREFILL, TaskStep::PRE, request->remote,
              -1, -1, {rid}};
        }
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
  using ReadySet = std::set<ReadyKey>;
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
      // Current-ready compute throughput only; add transfer-aware
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

  bool usesJointDecodePlan() const {
    return scoring_.throughput_weight > scoring_.waiting_weight &&
        !pipeline_plans_.empty() &&
        pipeline_plans_.back().active_remotes <
          system_.remote_count &&
        system_.latency_in_ms >=
          system_.schedule_cost + lookupTime(decode_proc_, 1);
  }

  void preparePipelinePlans() {
    pipeline_plans_.resize(kMaxRequests + 1);
    PipelinePlan best;
    for (int batch = 1; batch <= kMaxRequests; ++batch) {
      PipelinePlan best_for_batch;
      best_for_batch.batch_size = batch;
      for (int remotes = 1;
         remotes <= std::min(system_.remote_count, batch); ++remotes) {
        const int remote_batch = (batch + remotes - 1) / remotes;
        const long double edge_service =
          2.0L * system_.schedule_cost +
          lookupTime(decode_pre_, batch) +
          lookupTime(decode_post_, batch);
        const long double link_service =
          remotes * static_cast<long double>(system_.latency_in_ms) +
          8.0L * batch * system_.bytes_per_token /
            (static_cast<long double>(system_.bandwidth_gbps) *
             1.0e6L);
        const long double remote_service =
          system_.schedule_cost +
          lookupTime(decode_proc_, remote_batch);
        const long double period =
          std::max({edge_service, link_service, remote_service});
        const long double throughput = batch / period;
        if (throughput > best_for_batch.throughput) {
          best_for_batch.active_remotes = remotes;
          best_for_batch.throughput = throughput;
        }
      }
      if (best_for_batch.throughput > best.throughput) {
        best = best_for_batch;
      }
      pipeline_plans_[static_cast<std::size_t>(batch)] = best;
    }
  }

  bool prepareSchedulingData() {
    if (prefill_pre_.empty() || prefill_proc_.empty() ||
      prefill_post_.empty() || decode_pre_.empty() ||
      decode_proc_.empty() || decode_post_.empty()) {
      return false;
    }
    std::sort(prefill_pre_.begin(), prefill_pre_.end());
    std::sort(prefill_proc_.begin(), prefill_proc_.end());
    std::sort(prefill_post_.begin(), prefill_post_.end());
    std::sort(decode_pre_.begin(), decode_pre_.end());
    std::sort(decode_proc_.begin(), decode_proc_.end());
    std::sort(decode_post_.begin(), decode_post_.end());
    best_decode_pre_batch_ = bestBatchSizes(decode_pre_);
    best_decode_proc_batch_ = bestBatchSizes(decode_proc_);
    best_decode_post_batch_ = bestBatchSizes(decode_post_);
    preparePipelinePlans();
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

  int activeRequestCount() const {
    return static_cast<int>(std::count_if(
      requests_.begin(), requests_.end(),
      [](const std::optional<Request>& request) {
        return request && !request->finished;
      }));
  }

  const PipelinePlan& pipelinePlanForWorkload(int request_count) const {
    const int count = (std::max(request_count, 1) + 1) / 2;
    return pipeline_plans_[static_cast<std::size_t>(
      std::clamp(count, 1, kMaxRequests))];
  }

  const PipelinePlan& currentPipelinePlan() const {
    return pipelinePlanForWorkload(activeRequestCount());
  }

  int prefillRemoteCount() const {
    long double edge_work = 0.0L;
    long double uplink_work = 0.0L;
    long double remote_work = 0.0L;
    long double downlink_work = 0.0L;
    int remote_jobs = 0;
    for (const std::optional<Request>& request : requests_) {
      if (!request || request->finished ||
        !isPrefillStage(request->stage)) {
        continue;
      }
      if (request->stage == RequestStage::READY_P_PRE) {
        edge_work +=
          2.0L * system_.schedule_cost +
          lookupTime(prefill_pre_, request->input_length) +
          lookupTime(prefill_post_, request->input_length);
      } else if (request->stage < RequestStage::RUNNING_P_POST) {
        edge_work += system_.schedule_cost +
               lookupTime(prefill_post_, request->input_length);
      }
      if (request->stage <= RequestStage::WAITING_P_UP) {
        uplink_work += transferTime(request->input_length);
      }
      if (request->stage <= RequestStage::RUNNING_P_PROC) {
        remote_work +=
          system_.schedule_cost +
          lookupTime(prefill_proc_, request->input_length);
        ++remote_jobs;
      }
      if (request->stage <= RequestStage::WAITING_P_DOWN) {
        downlink_work += transferTime(request->input_length);
      }
    }
    const long double other_bottleneck =
      std::max({edge_work, uplink_work, downlink_work});
    if (remote_jobs == 0 || remote_work <= other_bottleneck) {
      return 1;
    }
    if (other_bottleneck <= 0.0L) {
      return std::min(system_.remote_count, remote_jobs);
    }
    const int required = static_cast<int>(
      std::ceil(remote_work / other_bottleneck));
    return std::clamp(required, 1,
             std::min(system_.remote_count, remote_jobs));
  }

  std::vector<int> decodePreBatch() const {
    if (!usesJointDecodePlan()) {
      return oldestBatch(ready_d_pre_, best_decode_pre_batch_);
    }

    const PipelinePlan& plan = currentPipelinePlan();
    const std::size_t target = std::min(
      ready_d_pre_.size(), static_cast<std::size_t>(plan.batch_size));
    std::vector<int> count(static_cast<std::size_t>(system_.remote_count));
    for (const ReadyKey& key : ready_d_pre_) {
      ++count[static_cast<std::size_t>(requestAt(key.rid)->remote)];
    }

    std::vector<bool> selected(
      static_cast<std::size_t>(system_.remote_count), false);
    const int oldest_remote = requestAt(ready_d_pre_.begin()->rid)->remote;
    selected[static_cast<std::size_t>(oldest_remote)] = true;
    std::vector<int> remotes;
    for (int remote = 0; remote < system_.remote_count; ++remote) {
      if (remote != oldest_remote &&
        count[static_cast<std::size_t>(remote)] > 0) {
        remotes.push_back(remote);
      }
    }
    std::sort(remotes.begin(), remotes.end(), [&](int lhs, int rhs) {
      return std::pair(-count[static_cast<std::size_t>(lhs)], lhs) <
          std::pair(-count[static_cast<std::size_t>(rhs)], rhs);
    });
    const int additional = std::min(
      plan.active_remotes - 1, static_cast<int>(remotes.size()));
    for (int index = 0; index < additional; ++index) {
      selected[static_cast<std::size_t>(remotes[index])] = true;
    }

    std::vector<int> request_ids;
    request_ids.reserve(target);
    for (const ReadyKey& key : ready_d_pre_) {
      const Request* request = requestAt(key.rid);
      if (selected[static_cast<std::size_t>(request->remote)]) {
        request_ids.push_back(request->rid);
        if (request_ids.size() == target) {
          break;
        }
      }
    }
    return request_ids;
  }

  std::vector<int> decodeProcBatch(const ReadySet& ready) const {
    if (!usesJointDecodePlan()) {
      return oldestBatch(ready, best_decode_proc_batch_);
    }

    std::map<int, std::vector<int>> groups;
    for (const ReadyKey& key : ready) {
      const Request* request = requestAt(key.rid);
      if (!invariant(request != nullptr && request->decode_wave >= 0)) {
        return {};
      }
      groups[request->decode_wave].push_back(request->rid);
    }
    const PipelinePlan& plan = currentPipelinePlan();
    const std::size_t target = static_cast<std::size_t>(
      (plan.batch_size + plan.active_remotes - 1) /
      plan.active_remotes);
    std::vector<int> request_ids;
    request_ids.reserve(std::min(target, ready.size()));
    for (const auto& [wave, group] : groups) {
      static_cast<void>(wave);
      if (!request_ids.empty() &&
        request_ids.size() + group.size() > target) {
        break;
      }
      request_ids.insert(request_ids.end(), group.begin(), group.end());
      if (request_ids.size() >= target) {
        break;
      }
    }
    return request_ids;
  }

  std::vector<int> completeDecodePostBatch() const {
    std::vector<int> request_ids;
    if (!usesJointDecodePlan() || complete_decode_waves_.empty()) {
      return request_ids;
    }
    const int target =
      pipelinePlanForWorkload(complete_decode_members_).batch_size;
    request_ids.reserve(static_cast<std::size_t>(
      std::min(target, complete_decode_members_)));
    for (int wave_id : complete_decode_waves_) {
      const auto wave = decode_waves_.find(wave_id);
      if (!invariant(wave != decode_waves_.end())) {
        return {};
      }
      if (!request_ids.empty() &&
        request_ids.size() + wave->second.request_ids.size() >
          static_cast<std::size_t>(target)) {
        break;
      }
      request_ids.insert(request_ids.end(),
                wave->second.request_ids.begin(),
                wave->second.request_ids.end());
      if (request_ids.size() >= static_cast<std::size_t>(target)) {
        break;
      }
    }
    return request_ids;
  }

  bool batchHasProducedToken(const std::vector<int>& request_ids) const {
    return std::any_of(request_ids.begin(), request_ids.end(),
              [&](int rid) {
                return requestAt(rid)->has_produced_token;
              });
  }

  bool shouldAccumulateFirstDecodeWave() const {
    if (!usesJointDecodePlan() || ready_d_pre_.empty() ||
      frontHasProducedToken(ready_d_pre_) ||
      ready_d_pre_.size() >= static_cast<std::size_t>(
                      currentPipelinePlan().batch_size)) {
      return false;
    }

    long double next_decode_ready =
      std::numeric_limits<long double>::infinity();
    for (const std::optional<Request>& request : requests_) {
      if (request && !request->finished &&
        request->stage == RequestStage::WAITING_P_DOWN) {
        next_decode_ready = std::min<long double>(
          next_decode_ready,
          request->transfer_ready_time + system_.schedule_cost +
            lookupTime(prefill_post_, request->input_length));
      }
    }
    if (!std::isfinite(next_decode_ready)) {
      return false;
    }

    const std::vector<int> batch = decodePreBatch();
    const long double task_done =
      current_time_ + system_.schedule_cost +
      lookupTime(decode_pre_, static_cast<int>(batch.size()));
    const long double uplink_done =
      std::max<long double>(task_done, uplink_free_time_) +
      transferTime(static_cast<int>(batch.size()));
    return next_decode_ready <= uplink_done;
  }

  void createDecodeWave(const std::vector<int>& request_ids) {
    const int wave_id = next_decode_wave_++;
    DecodeWave wave{request_ids, 0};
    for (int rid : request_ids) {
      Request* request = requestAt(rid);
      static_cast<void>(invariant(request != nullptr &&
                    request->decode_wave == -1));
      request->decode_wave = wave_id;
    }
    decode_waves_.emplace(wave_id, std::move(wave));
  }

  void markDecodePostReady(int rid) {
    Request* request = requestAt(rid);
    if (!usesJointDecodePlan() || request->decode_wave < 0) {
      return;
    }
    auto wave = decode_waves_.find(request->decode_wave);
    if (!invariant(wave != decode_waves_.end() &&
            wave->second.ready_post <
              static_cast<int>(wave->second.request_ids.size()))) {
      return;
    }
    ++wave->second.ready_post;
    if (wave->second.ready_post ==
      static_cast<int>(wave->second.request_ids.size())) {
      complete_decode_waves_.insert(wave->first);
      complete_decode_members_ += wave->second.ready_post;
    }
  }

  void detachDecodePostBatch(const std::vector<int>& request_ids) {
    std::map<int, std::vector<int>> selected_by_wave;
    for (int rid : request_ids) {
      Request* request = requestAt(rid);
      if (request->decode_wave >= 0) {
        selected_by_wave[request->decode_wave].push_back(rid);
      }
    }

    for (const auto& [wave_id, selected] : selected_by_wave) {
      auto wave = decode_waves_.find(wave_id);
      if (!invariant(wave != decode_waves_.end() &&
              wave->second.ready_post >=
                static_cast<int>(selected.size()))) {
        return;
      }
      if (complete_decode_waves_.erase(wave_id) != 0) {
        complete_decode_members_ -=
          static_cast<int>(wave->second.request_ids.size());
      }

      std::set<int> removed(selected.begin(), selected.end());
      std::vector<int> remaining;
      remaining.reserve(wave->second.request_ids.size() -
               selected.size());
      for (int rid : wave->second.request_ids) {
        if (removed.contains(rid)) {
          requestAt(rid)->decode_wave = -1;
        } else {
          remaining.push_back(rid);
        }
      }
      wave->second.ready_post -= static_cast<int>(selected.size());
      wave->second.request_ids = std::move(remaining);
      if (wave->second.request_ids.empty()) {
        decode_waves_.erase(wave);
      } else if (wave->second.ready_post ==
            static_cast<int>(wave->second.request_ids.size())) {
        complete_decode_waves_.insert(wave_id);
        complete_decode_members_ += wave->second.ready_post;
      }
    }
  }

  int placementRemoteLimit() const {
    return usesJointDecodePlan()
          ? std::max(currentPipelinePlan().active_remotes,
               prefillRemoteCount())
          : system_.remote_count;
  }

  int leastLoadedRemote(int remote_limit) const {
    // O(R) for each of at most R assignments; maintain counters
    // only if the request limit grows beyond 2000.
    std::vector<int> load(static_cast<std::size_t>(system_.remote_count));
    for (const std::optional<Request>& request : requests_) {
      if (request && !request->finished && request->remote >= 0) {
        ++load[static_cast<std::size_t>(request->remote)];
      }
    }

    const int first = next_remote_ % remote_limit;
    int best = first;
    for (int offset = 1; offset < remote_limit; ++offset) {
      const int remote = (first + offset) % remote_limit;
      if (load[static_cast<std::size_t>(remote)] <
        load[static_cast<std::size_t>(best)]) {
        best = remote;
      }
    }
    return best;
  }

  static bool isPrefillStage(RequestStage stage) {
    return stage >= RequestStage::READY_P_PRE &&
        stage <= RequestStage::RUNNING_P_POST;
  }

  long double transferTime(int token_count) const {
    return system_.latency_in_ms +
        8.0L * token_count * system_.bytes_per_token /
          (static_cast<long double>(system_.bandwidth_gbps) * 1.0e6L);
  }

  long double estimatedPrefillTime(const Request& request) const {
    return 3.0L * system_.schedule_cost +
        lookupTime(prefill_pre_, request.input_length) +
        lookupTime(prefill_proc_, request.input_length) +
        lookupTime(prefill_post_, request.input_length) +
        2.0L * transferTime(request.input_length);
  }

  const Request* shortestPrefill(const ReadySet& ready) const {
    // O(n) per admission is bounded by kMaxRequests; maintain a
    // second cost-ordered index only if that limit grows substantially.
    const Request* best = nullptr;
    long double best_time = 0.0L;
    for (const ReadyKey& key : ready) {
      const Request* candidate = requestAt(key.rid);
      if (!invariant(candidate != nullptr)) {
        return nullptr;
      }
      const long double time = estimatedPrefillTime(*candidate);
      if (best == nullptr || time < best_time ||
        (time == best_time &&
         std::pair(candidate->arrival_time, candidate->rid) <
           std::pair(best->arrival_time, best->rid))) {
        best = candidate;
        best_time = time;
      }
    }
    return best;
  }

  double normalizedLateness(const Request& request) const {
    // Starvation guard only: the scorer uses aggregate mean TDR/TPOT, not
    // per-request deadlines.
    const bool prefill = isPrefillStage(request.stage);
    if (!prefill && !request.has_produced_token) {
      return -std::numeric_limits<double>::infinity();
    }
    const double target = prefill ? scoring_.slo1 : scoring_.slo2;
    const double reference =
      prefill ? request.arrival_time : request.decode_ready_time;
    return (current_time_ - reference - target) / target;
  }

  bool costlyPrefill(const Request& request) const {
    const long double transfer = transferTime(request.input_length);
    const double pre = system_.schedule_cost +
              lookupTime(prefill_pre_, request.input_length);
    const double proc = system_.schedule_cost +
              lookupTime(prefill_proc_, request.input_length);
    const double post = system_.schedule_cost +
              lookupTime(prefill_post_, request.input_length);
    return transfer > scoring_.slo2 || pre > scoring_.slo2 ||
        proc > scoring_.slo2 || post > scoring_.slo2;
  }

  bool shouldDeferPrefill(const Request& candidate) const {
    if (scoring_.throughput_weight != 0.0 ||
      normalizedLateness(candidate) >= 0.0 ||
      !costlyPrefill(candidate)) {
      return false;
    }

    bool active_decode = false;
    bool costly_prefill_in_flight = false;
    bool guaranteed_event = edge_task_.has_value() ||
                std::any_of(remote_tasks_.begin(), remote_tasks_.end(),
                      [](const auto& task) { return task.has_value(); });
    for (const std::optional<Request>& request : requests_) {
      if (!request || request->finished) {
        continue;
      }
      active_decode |=
        request->has_produced_token &&
        request->stage >= RequestStage::READY_D_PRE &&
        request->stage <= RequestStage::RUNNING_D_POST;
      costly_prefill_in_flight |=
        request->stage > RequestStage::READY_P_PRE &&
        request->stage <= RequestStage::RUNNING_P_POST &&
        costlyPrefill(*request);
      guaranteed_event |=
        request->stage == RequestStage::WAITING_P_UP ||
        request->stage == RequestStage::WAITING_P_DOWN ||
        request->stage == RequestStage::WAITING_D_UP ||
        request->stage == RequestStage::WAITING_D_DOWN;
    }

    // One costly prefill may overlap decode; raise the cap only
    // after isolated judge data shows that more link concurrency is useful.
    return active_decode && costly_prefill_in_flight && guaranteed_event;
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
      const double threshold =
        request->stage == RequestStage::READY_P_PRE &&
            scoring_.waiting_weight != 0.0
          ? std::max(1.0, scoring_.distance_base)
          : 0.0;
      if (lateness < threshold) {
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
    int count = 0;
    for (const std::optional<Request>& request : requests_) {
      if (request && !request->finished && request->stage == stage &&
        (remote < 0 || request->remote == remote)) {
        ++count;
      }
    }
    return count;
  }

  bool shouldDeferBatch(const ReadySet& ready,
             const std::vector<int>& batch_choices,
             const TimingCurve& curve,
             RequestStage pending_stage, int remote,
             bool already_deferred) const {
    if (already_deferred || ready.empty() ||
      mostOverdue({&ready}) != nullptr) {
      return false;
    }
    const int pending = pendingCount(pending_stage, remote);
    const std::size_t potential_size =
      std::min(batch_choices.size() - 1,
           ready.size() + static_cast<std::size_t>(pending));
    if (batch_choices[potential_size] <= batch_choices[ready.size()]) {
      return false;
    }
    if (scoring_.throughput_weight <= scoring_.waiting_weight) {
      return true;
    }

    double next_transfer = std::numeric_limits<double>::infinity();
    for (const std::optional<Request>& request : requests_) {
      if (request && !request->finished &&
        request->stage == pending_stage &&
        (remote < 0 || request->remote == remote)) {
        next_transfer =
          std::min(next_transfer, request->transfer_ready_time);
      }
    }
    const int batch = batch_choices[ready.size()];
    const long double current_done =
      current_time_ + system_.schedule_cost + lookupTime(curve, batch);
    return next_transfer <= current_done;
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
    const bool prefill = isPrefillStage(request.stage);
    const double priority_time =
      prefill ? request.arrival_time : request.decode_ready_time;
    return {!prefill && !request.has_produced_token, priority_time,
        request.rid};
  }

  bool frontHasProducedToken(const ReadySet& ready) const {
    if (ready.empty()) {
      return false;
    }
    const Request* request = requestAt(ready.begin()->rid);
    return invariant(request != nullptr) && request->has_produced_token;
  }

  ReadySet* readySetFor(const Request& request) {
    switch (request.stage) {
      case RequestStage::READY_P_PRE:
        return &ready_p_pre_;
      case RequestStage::READY_P_PROC:
        return &ready_p_proc_[static_cast<std::size_t>(request.remote)];
      case RequestStage::READY_P_POST:
        return &ready_p_post_;
      case RequestStage::READY_D_PRE:
        return &ready_d_pre_;
      case RequestStage::READY_D_PROC:
        return &ready_d_proc_[static_cast<std::size_t>(request.remote)];
      case RequestStage::READY_D_POST:
        return &ready_d_post_;
      default:
        return nullptr;
    }
  }

  void addReady(const Request& request) {
    ReadySet* ready = readySetFor(request);
    if (ready != nullptr) {
      static_cast<void>(invariant(ready->insert(readyKey(request)).second));
    }
  }

  void removeReady(const Request& request) {
    ReadySet* ready = readySetFor(request);
    if (ready != nullptr) {
      static_cast<void>(invariant(ready->erase(readyKey(request)) == 1));
    }
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
    return static_cast<RequestStage>(
      static_cast<int>(readyStageFor(task)) + 1);
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
      if (!invariant(!edge_task_)) {
        return false;
      }
    } else {
      const std::size_t remote = static_cast<std::size_t>(assignment.server);
      if (!invariant(!remote_tasks_[remote])) {
        return false;
      }
    }

    if (assignment.task.work == WorkKind::DECODE &&
      assignment.task.step == TaskStep::PRE &&
      usesJointDecodePlan()) {
      createDecodeWave(assignment.task.request_ids);
    } else if (assignment.task.work == WorkKind::DECODE &&
          assignment.task.step == TaskStep::POST) {
      detachDecodePostBatch(assignment.task.request_ids);
    }

    if (assignment.server == -1) {
      edge_task_ = assignment.task;
    } else {
      const std::size_t remote = static_cast<std::size_t>(assignment.server);
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

  void recordTransfer(Direction direction, int token_count,
            const std::vector<int>& request_ids,
            double timestamp) {
    double& link_free = direction == Direction::UP ? uplink_free_time_
                            : downlink_free_time_;
    link_free = static_cast<double>(
      std::max<long double>(timestamp, link_free) +
      transferTime(token_count));
    for (int rid : request_ids) {
      requestAt(rid)->transfer_ready_time = link_free;
    }
  }

  void recordTaskTransfers(const TaskSpec& task, double timestamp) {
    if (task.work == WorkKind::PREFILL) {
      const Request* request = requestAt(task.request_ids.front());
      if (task.step == TaskStep::PRE) {
        recordTransfer(Direction::UP, request->input_length,
                task.request_ids, timestamp);
      } else if (task.step == TaskStep::PROC &&
            task.layer_end == system_.num_layers) {
        recordTransfer(Direction::DOWN, request->input_length,
                task.request_ids, timestamp);
      }
      return;
    }

    if (task.step == TaskStep::PRE) {
      std::map<int, std::vector<int>> by_remote;
      for (int rid : task.request_ids) {
        by_remote[requestAt(rid)->remote].push_back(rid);
      }
      for (const auto& [remote, request_ids] : by_remote) {
        static_cast<void>(remote);
        recordTransfer(Direction::UP,
                static_cast<int>(request_ids.size()),
                request_ids, timestamp);
      }
    } else if (task.step == TaskStep::PROC) {
      recordTransfer(Direction::DOWN,
              static_cast<int>(task.request_ids.size()),
              task.request_ids, timestamp);
    }
  }

  bool applyTaskDone(const Event& event, double timestamp) {
    if (!validateTaskShape(event.task) ||
      !invariant(event.server == expectedServer(event.task)) ||
      !validateRequests(event.task, runningStageFor(event.task))) {
      return false;
    }

    if (event.server == -1) {
      if (!invariant(edge_task_ && *edge_task_ == event.task)) {
        return false;
      }
      edge_task_.reset();
    } else {
      const std::size_t remote = static_cast<std::size_t>(event.server);
      if (!invariant(remote < remote_tasks_.size() && remote_tasks_[remote] &&
              *remote_tasks_[remote] == event.task)) {
        return false;
      }
      remote_tasks_[remote].reset();
    }

    recordTaskTransfers(event.task, timestamp);

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
        request->has_produced_token |=
          event.task.work == WorkKind::DECODE;
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
      requestAt(rid)->transfer_ready_time = 0.0;
      if (next == RequestStage::READY_D_POST) {
        markDecodePostReady(rid);
      }
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
  TimingCurve prefill_pre_;
  TimingCurve prefill_proc_;
  TimingCurve prefill_post_;
  TimingCurve decode_pre_;
  TimingCurve decode_proc_;
  TimingCurve decode_post_;
  std::vector<int> best_decode_pre_batch_;
  std::vector<int> best_decode_proc_batch_;
  std::vector<int> best_decode_post_batch_;
  std::vector<PipelinePlan> pipeline_plans_;

  std::vector<std::optional<Request>> requests_;
  double current_time_ = 0.0;
  double uplink_free_time_ = 0.0;
  double downlink_free_time_ = 0.0;
  std::vector<bool> yield_to_decode_;
  std::vector<bool> deferred_d_proc_;
  bool deferred_d_post_ = false;
  std::optional<TaskSpec> edge_task_;
  std::vector<std::optional<TaskSpec>> remote_tasks_;
  int next_remote_ = 0;
  int next_decode_wave_ = 0;
  std::map<int, DecodeWave> decode_waves_;
  std::set<int> complete_decode_waves_;
  int complete_decode_members_ = 0;

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
