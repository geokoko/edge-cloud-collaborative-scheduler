#include <bits/stdc++.h>

using namespace std;
using usize = size_t;
using Real = long double;
constexpr Real kInfinity = numeric_limits<Real>::infinity();

enum class RequestStage {
 READY_P_PRE, RUNNING_P_PRE, WAITING_P_UP,
 READY_P_PROC, RUNNING_P_PROC, WAITING_P_DOWN,
 READY_P_POST, RUNNING_P_POST,
 READY_D_PRE, RUNNING_D_PRE, WAITING_D_UP,
 READY_D_PROC, RUNNING_D_PROC, WAITING_D_DOWN,
 READY_D_POST, RUNNING_D_POST,
 FINISHED,
};

enum class WorkKind { PREFILL, DECODE };
enum class TaskStep { PRE, PROC, POST };
enum class EventKind { ARRIVAL, TASK_DONE, TRANSFER_DONE, FINISH };
enum class Direction { UP, DOWN };
enum class TransferKind { PREFILL, DECODE };

using enum RequestStage; using enum WorkKind; using enum TaskStep;
using enum EventKind; using enum Direction;

struct TaskSpec {
 WorkKind work = PREFILL;
 TaskStep step = PRE;
 int remote = -1, layer_start = -1, layer_end = -1;
 vector<int> rids;
 bool operator==(const TaskSpec &) const = default;
};

struct Assignment { int server = -1; TaskSpec task; };

struct Event {
 EventKind kind = ARRIVAL;
 int rid = -1, input_len = 0, server = -1;
 TaskSpec task;
 double duration = 0.0;
 Direction direction = UP;
 TransferKind transfer_kind = TransferKind::PREFILL;
 int remote = -1;
 int64_t size = 0;
 vector<int> rids;
};

struct Request {
 int rid = -1, input_len = 0, remote = -1;
 RequestStage stage = READY_P_PRE;
 double arrival_time = 0.0;
 double decode_ready = 0.0;
 bool finished = false, has_token = false;
 int next_layer = 0;
 double transfer_ready = 0.0;
 int decode_wave = -1;
};

struct SystemConfig {
 int remotes = 0, token_bytes = 0, layers = 0;
 double schedule_ms = 0.0, latency_ms = 0.0, gbps = 0.0;
};

struct ScoringConfig {
 double slo1 = 0.0, slo2 = 0.0;
 double tp_ub = 0.0, tp_base = 0.0, dist_base = 0.0;
 double w_tp = 0.0, w_c = 0.0;
};

struct ReadyKey {
 bool first_token_decode = false;
 double priority_time = 0.0;
 int rid = -1;
 auto operator<=>(const ReadyKey &) const = default;
};

struct PipelinePlan {
 int batch_size = 1, proc_batch = 1, post_batch = 1, active_remotes = 1;
 Real quality = -kInfinity, finish = kInfinity;
};

struct DecodeWave { vector<int> rids; int ready_post = 0; };

class Scheduler {
 public:
 Scheduler(istream &input, ostream &output) : input_(input), output_(output) {}
 bool readStartup() {
  if(!(input_ >> sys_.remotes >> sys_.schedule_ms >> sys_.latency_ms >> sys_.gbps >> sys_.token_bytes >> sys_.layers)) {
   return false;
  }
  if(!(input_ >> score_.slo1 >> score_.slo2 >> score_.tp_ub >> score_.tp_base >> score_.dist_base >> score_.w_tp >> score_.w_c)) {
   return false;
  }
  int row_count = 0;
  if(!(input_ >> row_count) || row_count < 0) {
   return false;
  }
  for(int row = 0; row < row_count; ++row) {
   int batch_size = 0;
   double prefill_pre = 0.0, prefill_proc = 0.0, prefill_post = 0.0;
   double decode_pre = 0.0, decode_proc = 0.0, decode_post = 0.0;
   if(!(input_ >> batch_size >> prefill_pre >> prefill_proc >> prefill_post >> decode_pre >> decode_proc >> decode_post)) {
    return false;
   }
   if(prefill_pre >= 0.0) {
    prefill_pre_.emplace_back(batch_size, prefill_pre);
   }
   if(prefill_proc >= 0.0) {
    prefill_proc_.emplace_back(batch_size, prefill_proc);
   }
   if(prefill_post >= 0.0) {
    prefill_post_.emplace_back(batch_size, prefill_post);
   }
   if(decode_pre >= 0.0) {
    decode_pre_.emplace_back(batch_size, decode_pre);
   }
   if(decode_proc >= 0.0) {
    decode_proc_.emplace_back(batch_size, decode_proc);
   }
   if(decode_post >= 0.0) {
    decode_post_.emplace_back(batch_size, decode_post);
   }
  }
  if(sys_.remotes <= 0 || sys_.layers <= 0 || sys_.token_bytes <= 0 || score_.slo1 <= 0.0 || score_.slo2 <= 0.0) {
   return false;
  }
  if(!prepareSchedulingData()) {
   return false;
  }
  yield_to_decode_.assign(usize(sys_.remotes), false);
  remote_tasks_.resize(usize(sys_.remotes));
  remote_free_at_.assign(sys_.remotes, 0);
  ready_p_proc_.resize(usize(sys_.remotes));
  ready_d_proc_.resize(usize(sys_.remotes));
  active_by_remote_.assign(sys_.remotes, 0);
  prefill_load_.assign(sys_.remotes, 0);
  input_.ignore(numeric_limits<streamsize>::max(), '\n');
  return true;
 }
 bool readAndProcessFrame() {
  string header;
  if(!getline(input_, header) || header == "END") {
   return false;
  }
  double timestamp = 0.0;
  if(!parseSingleValue(header, timestamp) || !isfinite(timestamp)) {
   return false;
  }
  string count_line;
  int event_count = 0;
  if(!getline(input_, count_line) || !parseSingleValue(count_line, event_count) || event_count < 0) {
   return false;
  }
  vector<Event> events;
  events.reserve(usize(event_count));
  for(int i = 0; i < event_count; ++i) {
   string line;
   if(!getline(input_, line)) {
    return false;
   }
   optional<Event> event = parseEvent(line);
   if(!event) {
    return false;
   }
   events.push_back(move(*event));
  }
  now_ = timestamp;
  for(const Event &event : events) {
   if(event.kind != FINISH && !applyEvent(event, timestamp)) {
    return false;
   }
  }
  for(const Event &event : events) {
   if(event.kind == FINISH && !applyFinish(event.rid)) {
    return false;
   }
  }
  vector<Assignment> assignments = chooseAssignments();
  emitAssignments(assignments);
  return bool(output_);
 }

 vector<Assignment> chooseAssignments() {
  vector<Assignment> assignments;
  assignments.reserve(usize(sys_.remotes + 1));

  for(int remote = 0; remote < sys_.remotes; ++remote) {
   if(remote_tasks_[usize(remote)])
    continue;

   TaskSpec task;
   bool found = false;
   const ReadySet &decode = ready_d_proc_[usize(remote)];
   const ReadySet &prefill = ready_p_proc_[usize(remote)];
   const bool active_decode = frontHasProducedToken(decode);
   const int proc_target = decode.empty() ? 1 : plannedTarget(decode, pipelinePlan().proc_batch, best_proc_batch_);
   const bool defer_decode = !usesJointPlan() && !decode.empty() && shouldDeferBatch(decode, proc_target, decode_proc_, WAITING_D_UP, remote);

   if(yield_to_decode_[usize(remote)] && !decode.empty()) {
    task = {DECODE, PROC, remote, -1, -1, decodeProcBatch(decode)};
    found = true;
   } else {
    const Request *overdue = mostOverdue({&decode, &prefill});
    if(overdue && overdue->stage == READY_P_PROC) {
     int layer_end = sys_.layers;
     if(active_decode && overdue->next_layer == 0 && sys_.layers > 1) {
     layer_end = (sys_.layers + 1) / 2;
     }
     task = {PREFILL, PROC, remote, overdue->next_layer, layer_end, {overdue->rid}};
     found = true;
    } else if(!decode.empty() && (active_decode || prefill.empty()) && !defer_decode) {
     task = {DECODE, PROC, remote, -1, -1, decodeProcBatch(decode)};
     found = true;
    } else if(!prefill.empty()) {
     const Request *request = requestAt(prefill.begin()->rid);
     if(!invariant(request != nullptr))
     return assignments;
     int layer_end = prefillLayerEnd(*request, remote);
     const bool hold_down = score_.w_c > 0.0 && normalizedLateness(*request) < 0.0 && transferTime(request->input_len) > score_.slo2 / score_.w_c &&
     any_of(requests_.begin(), requests_.end(), [](const optional<Request> &other) {
     return other && !other->finished && other->has_token && other->stage >= READY_D_PRE && other->stage <= RUNNING_D_POST;
     });
     if(hold_down && layer_end == sys_.layers)
     --layer_end;
     if(layer_end == request->next_layer)
     continue;
     task = {PREFILL, PROC, remote, request->next_layer, layer_end, {request->rid}};
     found = true;
    }
   }

   if(found) {
    Assignment assignment{remote, move(task)};
    if(!startAssignment(assignment))
     return assignments;
    yield_to_decode_[usize(remote)] = false;
    assignments.push_back(move(assignment));
   }
  }

  if(!edge_task_) {
   TaskSpec task;
   const Request *overdue = mostOverdue({&ready_d_post_, &ready_p_post_, &ready_d_pre_, &ready_p_pre_});
   vector<int> decode_post_batch;
   if(usesJointPlan()) {
    if(overdue && overdue->stage == READY_D_POST) {
     const auto wave = waves_.find(overdue->decode_wave);
     if(wave != waves_.end() && complete_waves_.contains(wave->first)) {
     decode_post_batch = wave->second.rids;
     } else {
     decode_post_batch = {overdue->rid};
     }
    } else {
     decode_post_batch = completeDecodePostBatch();
    }
   }
   const bool decode_post_available = usesJointPlan() ? !decode_post_batch.empty() : !ready_d_post_.empty();
   const int post_target = ready_d_post_.empty() ? 1 : plannedTarget(ready_d_post_, pipelinePlan().post_batch, best_post_batch_);
   const bool defer_decode_post = !usesJointPlan() && decode_post_available &&
     (shouldDeferBatch(ready_d_post_, post_target, decode_post_, WAITING_D_DOWN, -1) ||
     (ready_d_post_.size() < usize(post_target) && (!ready_d_pre_.empty() || !ready_p_post_.empty()) &&
     (favorsThroughput() || mostOverdue({&ready_d_post_}) == nullptr)));
   const bool decode_post_schedulable = decode_post_available && !defer_decode_post;
   const bool active_d_post = decode_post_schedulable && (usesJointPlan() ? batchHasProducedToken(decode_post_batch) : frontHasProducedToken(ready_d_post_));
   const bool active_d_pre = frontHasProducedToken(ready_d_pre_);
   const int fill_target = active_d_pre ? pipelinePlan().batch_size : 1;
   const bool fill_decode_pre = !deferred_d_pre_ && active_d_pre && score_.w_tp >= 3.0 * score_.w_c && ready_d_pre_.size() < usize(fill_target);
   optional<RequestStage> selected_stage;
   if(overdue != nullptr && !(overdue->stage == READY_D_POST && defer_decode_post)) {
    selected_stage = overdue->stage;
   } else if(active_d_post) {
    selected_stage = READY_D_POST;
   } else if(fill_decode_pre && !ready_p_post_.empty()) {
    selected_stage = READY_P_POST;
   } else if(fill_decode_pre && decode_post_schedulable && canFillDecodePost(decode_post_batch)) {
    selected_stage = READY_D_POST;
   } else if(fill_decode_pre && !ready_p_pre_.empty() && ready_p_pre_short_.begin()->first <= score_.slo1) {
    selected_stage = READY_P_PRE;
   } else if(active_d_pre) {
    selected_stage = READY_D_PRE;
   } else if(!ready_p_post_.empty()) {
    selected_stage = READY_P_POST;
   } else if(!ready_p_pre_.empty()) {
    selected_stage = READY_P_PRE;
   } else if(decode_post_schedulable) {
    selected_stage = READY_D_POST;
   } else if(!ready_d_pre_.empty()) {
    selected_stage = READY_D_PRE;
   }
   if(selected_stage == READY_D_PRE && shouldAccumulateFirstDecodeWave()) {
    if(!ready_p_post_.empty())
     selected_stage = READY_P_POST;
    else if(!ready_p_pre_.empty())
     selected_stage = READY_P_PRE;
    else
     selected_stage.reset();
   }
   if(selected_stage == READY_D_POST) {
    if(usesJointPlan()) {
     if(!invariant(!decode_post_batch.empty())) {
     return assignments;
     }
     task = {DECODE, POST, -1, -1, -1, move(decode_post_batch)};
    } else {
     task = {DECODE, POST, -1, -1, -1, oldestBatch(ready_d_post_, post_target)};
    }
   } else if(selected_stage == READY_P_POST) {
    const int rid = ready_p_post_.begin()->rid;
    task = {PREFILL, POST, requestAt(rid)->remote, -1, -1, {rid}};
   } else if(selected_stage == READY_D_PRE) {
    task = {DECODE, PRE, -1, -1, -1, decodePreBatch()};
   } else if(selected_stage == READY_P_PRE) {
    const Request *candidate = overdue != nullptr && overdue->stage == READY_P_PRE && score_.w_c && normalizedLateness(*overdue) >= max(1.0, score_.dist_base)
     ? overdue
     : requestAt(ready_p_pre_short_.begin()->second);
    if(!invariant(candidate != nullptr)) {
     return assignments;
    }
    const int rid = candidate->rid;
    Request *request = requestAt(rid);
    if(!invariant(request != nullptr && request->remote == -1)) {
     return assignments;
    }
    if(shouldDeferPrefill(*request)) {
     selected_stage.reset();
    } else {
     const int remote_limit = placementRemoteLimit();
     request->remote = leastLoadedRemote(remote_limit);
     next_remote_ = (request->remote + 1) % remote_limit;
     task = {PREFILL, PRE, request->remote, -1, -1, {rid}};
    }
   }
   if(selected_stage) {
    Assignment assignment{-1, move(task)};
    if(!startAssignment(assignment))
     return assignments;
    if(assignment.task.work == DECODE && assignment.task.step == PRE)
     deferred_d_pre_ = false;
    else if(active_d_pre)
     deferred_d_pre_ = true;
    assignments.push_back(move(assignment));
   }
  }
  return assignments;
 }

 void emitAssignments(const vector<Assignment> &assignments) {
  output_ << assignments.size() << '\n';
  for(const Assignment &assignment : assignments) {
   if(assignment.server == -1)
    output_ << 'E';
   else
    output_ << 'C' << assignment.server;
   output_ << ' ';
   writeTaskSpec(assignment.task);
   output_ << '\n';
  }
  output_ << flush;
 }

 private:
 using ReadySet = set<ReadyKey>;
 using TimingCurve = vector<pair<int, double>>;
 static constexpr int kMaxRequests = 2000;
 static bool invariant(bool condition) {
  assert(condition);
  return condition;
 }
 static double taskTime(const TimingCurve &curve, int batch_size) {
  const auto upper = lower_bound(curve.begin(), curve.end(), batch_size, [](const auto &point, int size) { return point.first < size; });
  if(upper == curve.begin()) {
   return upper->second;
  }
  if(upper == curve.end()) {
   return curve.back().second;
  }
  if(upper->first == batch_size) {
   return upper->second;
  }
  const auto lower = prev(upper);
  const double position = double(batch_size - lower->first) / (upper->first - lower->first);
  return lower->second + position * (upper->second - lower->second);
 }
 vector<int> bestBatchSizes(const TimingCurve &curve) const {
  vector<int> choices(kMaxRequests + 1, 1);
  Real best_rate = -1.0L;
  int best_size = 1;
  for(int size = 1; size <= kMaxRequests; ++size) {
   const Real rate = size / (Real(sys_.schedule_ms) + taskTime(curve, size));
   if(rate > best_rate) {
    best_rate = rate;
    best_size = size;
   }
   choices[usize(size)] = best_size;
  }
  return choices;
 }
 bool usesJointPlan() const { return score_.w_tp > score_.w_c && sys_.remotes > 1 && sys_.latency_ms >= sys_.schedule_ms + taskTime(decode_proc_, 1); }
 bool favorsThroughput() const { return score_.w_c == 0 || (score_.dist_base > 0 && score_.w_c / score_.dist_base < score_.w_tp); }
 Real frontierRelease(const Request &r) const {
  Real edge = edge_task_ ? max(0.L, (Real)edge_free_at_ - now_) : 0;
  Real remote = r.remote >= 0 && remote_tasks_[r.remote] ? max(0.L, (Real)remote_free_at_[r.remote] - now_) : 0;
  Real post = sys_.schedule_ms + taskTime(prefill_post_, r.input_len);
  Real proc = sys_.schedule_ms + taskTime(prefill_proc_, r.input_len) * (sys_.layers - r.next_layer) / sys_.layers;
  Real transfer = transferTime(r.input_len);
  Real dpost = sys_.schedule_ms + taskTime(decode_post_, 1);
  Real dproc = sys_.schedule_ms + taskTime(decode_proc_, 1);
  Real dtransfer = transferTime(1);
  switch(r.stage) {
  case READY_P_PRE:
   return edge + sys_.schedule_ms + taskTime(prefill_pre_, r.input_len) + 2 * transfer + proc + post;
  case RUNNING_P_PRE:
   return max(0.L, (Real)edge_free_at_ - now_) + 2 * transfer + proc + post;
  case WAITING_P_UP:
   return max(0.L, (Real)r.transfer_ready - now_) + proc + transfer + post;
  case READY_P_PROC:
   return remote + proc + transfer + post;
  case RUNNING_P_PROC:
   return max(0.L, (Real)remote_free_at_[r.remote] - now_) + transfer + post;
  case WAITING_P_DOWN:
   return max(0.L, (Real)r.transfer_ready - now_) + post;
  case READY_P_POST:
   return edge + post;
  case RUNNING_P_POST:
   return max(0.L, (Real)edge_free_at_ - now_);
  case READY_D_PRE:
   return 0;
  case RUNNING_D_PRE:
   return max(0.L, (Real)edge_free_at_ - now_) + dtransfer + dproc + dtransfer + dpost;
  case WAITING_D_UP:
   return max(0.L, (Real)r.transfer_ready - now_) + dproc + dtransfer + dpost;
  case READY_D_PROC:
   return remote + dproc + dtransfer + dpost;
  case RUNNING_D_PROC:
   return max(0.L, (Real)remote_free_at_[r.remote] - now_) + dtransfer + dpost;
  case WAITING_D_DOWN:
   return max(0.L, (Real)r.transfer_ready - now_) + dpost;
  case READY_D_POST:
   return edge + dpost;
  case RUNNING_D_POST:
   return max(0.L, (Real)edge_free_at_ - now_);
  default:
   return 0;
  }
 }

 pair<Real, Real> simulateDecodePlan(vector<pair<Real, int>> jobs, int pre_batch, int proc_batch, int post_batch, int remote_limit, int cycles,
     int first_batch = 0) const {
  const bool joint = usesJointPlan();
  struct Item {
   int order, generation, remote, wave, job;
  };
  struct Group {
   vector<Item> items;
  };
  array<int, 8> placement{};
  for(auto [release, remote] : jobs)
   if(remote >= 0)
    ++placement[remote];
  for(auto &job : jobs)
   if(job.second < 0) {
    int best = 0;
    for(int r = 1; r < remote_limit; ++r) {
     if(placement[r] < placement[best])
     best = r;
    }
    job.second = best;
    ++placement[best];
   }
  vector<Group> groups;
  using Ev = tuple<Real, int, int, int, int>;
  priority_queue<Ev, vector<Ev>, greater<Ev>> events;
  array<deque<Item>, 8> pre_ready, proc_ready;
  deque<Item> post_ready;
  deque<int> post_waves;
  vector<int> wave_total, wave_down;
  vector<vector<Item>> wave_items;
  array<bool, 8> remote_busy{};
  bool edge_busy = edge_task_.has_value();
  int sequence = 0, ready_pre = 0, remaining_pre = jobs.size(), remaining_post = 0, done_members = 0, next_active = -1;
  array<int, 8> remaining_proc{};
  for(auto job : jobs)
   ++remaining_proc[job.second];
  const int members = jobs.size() * cycles;
  bool first_wave = true;
  Real up = max(0.L, (Real)up_free_at_ - now_);
  Real down = max(0.L, (Real)down_free_at_ - now_);
  Real gap_sum = 0, completion_sum = 0, last = 0, now = 0;
  int gaps = 0;
  vector<Real> last_token(jobs.size(), -1);
  if(edge_busy) {
   events.emplace(max(0.L, (Real)edge_free_at_ - now_), sequence++, 5, -1, -1);
  }
  for(int r = 0; r < sys_.remotes; ++r)
   if(remote_tasks_[r]) {
    remote_busy[r] = true;
    events.emplace(max(0.L, (Real)remote_free_at_[r] - now_), sequence++, 6, -1, r);
   }
  for(int job = 0; job < (int)jobs.size(); ++job) {
   events.emplace(jobs[job].first, sequence++, 7, job, jobs[job].second);
  }
  auto save = [&](vector<Item> &&items) {
   groups.push_back({move(items)});
   return int(groups.size() - 1);
  };
  auto dispatch = [&](Real time) {
   for(int r = 0; r < sys_.remotes; ++r)
    if(!remote_busy[r] && !proc_ready[r].empty()) {
     int ready = proc_ready[r].size();
     int size = min(ready, proc_batch);
     if(joint) {
     size = 0;
     while(size < ready) {
     int end = size + 1;
     while(end < ready && proc_ready[r][end].wave == proc_ready[r][size].wave)
     ++end;
     if(size && end > proc_batch)
     break;
     size = end;
     if(size >= proc_batch)
     break;
     }
     } else if(ready < proc_batch && ready < remaining_proc[r])
     continue;
     vector<Item> items;
     for(int i = 0; i < size; ++i) {
     items.push_back(proc_ready[r].front());
     proc_ready[r].pop_front();
     }
     remaining_proc[r] -= size;
     remaining_post += size;
     int group = save(move(items));
     remote_busy[r] = true;
     events.emplace(time + sys_.schedule_ms + taskTime(decode_proc_, size), sequence++, 2, group, r);
    }
   if(edge_busy)
    return;
   int post_count = post_ready.size();
   if(joint && !post_waves.empty()) {
    vector<Item> items;
    do {
     int wave = post_waves.front();
     if(!items.empty() && items.size() + wave_items[wave].size() > usize(post_batch)) {
     break;
     }
     post_waves.pop_front();
     items.insert(items.end(), wave_items[wave].begin(), wave_items[wave].end());
    } while(!post_waves.empty());
    remaining_post -= items.size();
    int size = items.size(), group = save(move(items));
    edge_busy = true;
    events.emplace(time + sys_.schedule_ms + taskTime(decode_post_, size), sequence++, 4, group, -1);
   } else if(!joint && post_count && (post_count >= post_batch || post_count == remaining_post)) {
    int size = min(post_count, post_batch);
    vector<Item> items;
    for(int i = 0; i < size; ++i) {
     items.push_back(post_ready.front());
     post_ready.pop_front();
    }
    remaining_post -= size;
    int group = save(move(items));
    edge_busy = true;
    events.emplace(time + sys_.schedule_ms + taskTime(decode_post_, size), sequence++, 4, group, -1);
   } else if(ready_pre) {
    int target = first_wave && first_batch ? first_batch : pre_batch;
    if(ready_pre < target && ready_pre < remaining_pre)
     return;
    int oldest = -1;
    for(int r = 0; r < sys_.remotes; ++r) {
     if(!pre_ready[r].empty() && (oldest < 0 || pre_ready[r].front().order < pre_ready[oldest].front().order))
     oldest = r;
    }
    vector<int> remotes{oldest};
    while((int)remotes.size() < remote_limit) {
     int best = -1;
     for(int r = 0; r < sys_.remotes; ++r) {
     if(!pre_ready[r].empty() && find(remotes.begin(), remotes.end(), r) == remotes.end() &&
     (best < 0 || pair(-int(pre_ready[r].size()), r) < pair(-int(pre_ready[best].size()), best)))
     best = r;
     }
     if(best < 0)
     break;
     remotes.push_back(best);
    }
    vector<Item> items;
    while((int)items.size() < target) {
     int remote = -1;
     for(int r : remotes) {
     if(!pre_ready[r].empty() && (remote < 0 || pre_ready[r].front().order < pre_ready[remote].front().order))
     remote = r;
     }
     if(remote < 0)
     break;
     items.push_back(pre_ready[remote].front());
     pre_ready[remote].pop_front();
     --ready_pre;
    }
    remaining_pre -= items.size();
    int size = items.size(), wave = wave_total.size();
    for(Item &item : items)
     item.wave = wave;
    wave_total.push_back(size);
    wave_down.push_back(0);
    wave_items.push_back(items);
    int group = save(move(items));
    edge_busy = true;
    first_wave = false;
    events.emplace(time + sys_.schedule_ms + taskTime(decode_pre_, size), sequence++, 0, group, -1);
   }
  };
  vector<Ev> frame;
  dispatch(0);
  while(done_members < members) {
   if(events.empty()) {
    return {kInfinity, kInfinity};
   }
   now = get<0>(events.top());
   frame.clear();
   while(!events.empty() && get<0>(events.top()) == now) {
    frame.push_back(events.top());
    events.pop();
   }
   for(auto [time, seq, type, group, remote] : frame) {
    (void)seq;
    if(type == 0) {
     edge_busy = false;
     array<vector<Item>, 8> split;
     for(Item item : groups[group].items)
     split[item.remote].push_back(item);
     for(int r = 0; r < sys_.remotes; ++r)
     if(!split[r].empty()) {
     int child = save(move(split[r]));
     up = max(up, time) + transferTime(groups[child].items.size());
     events.emplace(up, sequence++, 1, child, r);
     }
    } else if(type == 1) {
     for(Item item : groups[group].items)
     proc_ready[remote].push_back(item);
    } else if(type == 2) {
     remote_busy[remote] = false;
     down = max(down, time) + transferTime(groups[group].items.size());
     events.emplace(down, sequence++, 3, group, remote);
    } else if(type == 3) {
     if(joint) {
     int wave = groups[group].items.front().wave;
     wave_down[wave] += groups[group].items.size();
     if(wave_down[wave] == wave_total[wave])
     post_waves.push_back(wave);
     } else
     for(Item item : groups[group].items)
     post_ready.push_back(item);
    } else if(type == 4) {
     edge_busy = false;
     int count = groups[group].items.size();
     done_members += count;
     last = time;
     completion_sum += time * count;
     for(Item item : groups[group].items) {
     if(last_token[item.job] >= 0) {
     gap_sum += time - last_token[item.job];
     ++gaps;
     }
     last_token[item.job] = time;
     }
     for(auto it = groups[group].items.rbegin(); it != groups[group].items.rend(); ++it)
     if(it->generation + 1 < cycles) {
     Item item{next_active--, it->generation + 1, it->remote, -1, it->job};
     pre_ready[item.remote].push_front(item);
     ++ready_pre;
     ++remaining_pre;
     ++remaining_proc[item.remote];
     }
    } else if(type == 5)
     edge_busy = false;
    else if(type == 6)
     remote_busy[remote] = false;
    else {
     Item item{group, 0, remote, -1, group};
     pre_ready[remote].push_back(item);
     ++ready_pre;
    }
   }
   dispatch(now);
  }
  Real tp = members / max(last, 1e-12L);
  Real norm_tp = clamp((tp - score_.tp_base) / (score_.tp_ub - score_.tp_base), 0.L, 1.L);
  Real mean = joint ? completion_sum / members : (gaps ? gap_sum / gaps : 0);
  Real excess = max(0.L, mean / score_.slo2 - 1);
  Real norm_c = score_.dist_base > 0 ? max(0.L, 1 - (joint ? 1.5L : 1.L) * excess / score_.dist_base) : (excess == 0 ? 1 : 0);
  return {score_.w_tp * norm_tp + score_.w_c * norm_c, last};
 }

 vector<pair<Real, int>> knownDecodeJobs() const {
  vector<pair<Real, int>> jobs;
  for(bool active : {true, false}) {
   for(const auto &request : requests_) {
    if(request && !request->finished && request->has_token == active) {
     jobs.emplace_back(frontierRelease(*request), request->remote);
    }
   }
  }
  stable_sort(jobs.begin(), jobs.end(), [](auto &a, auto &b) { return a.first < b.first; });
  if(jobs.size() > 256)
   jobs.resize(256);
  return jobs;
 }

 PipelinePlan planKnownDecodeWork() const {
  auto jobs = knownDecodeJobs();
  int total = max((int)jobs.size(), 1);
  bool strong = usesJointPlan() && score_.w_tp > 3 * score_.w_c;
  int cycles = clamp((strong ? 1280 : 1024) / total, 4, 16);

  set<int> sizes{1, total, (total + 1) / 2, best_pre_batch_[total], best_proc_batch_[total], best_post_batch_[total]};
  for(int groups = 1; groups <= min(total, 12); ++groups) {
   sizes.insert((total + groups - 1) / groups);
   sizes.insert(total / groups);
  }
  for(int size = 1; size < total; size *= 2)
   sizes.insert(size);

  PipelinePlan answer;
  auto better = [&](Real quality, Real finish) {
   if(!answer.finish || score_.w_tp > 3 * score_.w_c) {
    return !answer.finish || finish < answer.finish;
   }
   Real scale = max({1.L, abs(quality), abs(answer.quality)});
   return quality > answer.quality + 1e-12L * scale || (abs(quality - answer.quality) <= 1e-12L * scale && finish < answer.finish);
  };

  for(int remotes = 1; remotes <= min(sys_.remotes, total); ++remotes) {
   for(int size : sizes) {
    if(size <= 0 || size > total)
     continue;
    int proc = (size + remotes - 1) / remotes;
    auto [quality, finish] = simulateDecodePlan(jobs, size, proc, size, remotes, cycles);
    if(better(quality, finish)) {
     answer = {size, proc, size, remotes, quality, finish};
    }
   }
  }

  if(!usesJointPlan()) {
   set<int> proc_sizes{1, best_proc_batch_[total]};
   for(int size : sizes) {
    proc_sizes.insert((size + answer.active_remotes - 1) / answer.active_remotes);
   }
   bool changed = false;
   auto improve = [&](int pre, int proc, int post) {
    if(pre == answer.batch_size && proc == answer.proc_batch && post == answer.post_batch)
     return;
    auto [quality, finish] = simulateDecodePlan(jobs, pre, proc, post, answer.active_remotes, cycles);
    if(better(quality, finish)) {
     answer = {pre, proc, post, answer.active_remotes, quality, finish};
     changed = true;
    }
   };
   for(int pass = 0; pass < 2; ++pass) {
    changed = false;
    for(int size : sizes)
     improve(size, answer.proc_batch, answer.post_batch);
    for(int size : proc_sizes)
     improve(answer.batch_size, size, answer.post_batch);
    for(int size : sizes)
     improve(answer.batch_size, answer.proc_batch, size);
    if(!changed)
     break;
   }
  }

  if(active_count_ > total) {
   auto scale = [&](int size) { return min(active_count_, (size * active_count_ + total - 1) / total); };
   answer.batch_size = scale(answer.batch_size);
   answer.proc_batch = scale(answer.proc_batch);
   answer.post_batch = scale(answer.post_batch);
  }
  return answer;
 }

 bool prepareSchedulingData() {
  if(prefill_pre_.empty() || prefill_proc_.empty() || prefill_post_.empty() || decode_pre_.empty() || decode_proc_.empty() || decode_post_.empty()) {
   return false;
  }
  sort(prefill_pre_.begin(), prefill_pre_.end());
  sort(prefill_proc_.begin(), prefill_proc_.end());
  sort(prefill_post_.begin(), prefill_post_.end());
  sort(decode_pre_.begin(), decode_pre_.end());
  sort(decode_proc_.begin(), decode_proc_.end());
  sort(decode_post_.begin(), decode_post_.end());
  best_pre_batch_ = bestBatchSizes(decode_pre_);
  best_proc_batch_ = bestBatchSizes(decode_proc_);
  best_post_batch_ = bestBatchSizes(decode_post_);
  const auto minimum = [](const TimingCurve &curve) {
   return min_element(curve.begin(), curve.end(), [](const auto &lhs, const auto &rhs) { return lhs.second < rhs.second; })->second;
  };
  min_d_pre_ = minimum(decode_pre_);
  min_d_post_ = minimum(decode_post_);
  return true;
 }
 vector<int> oldestBatch(const ReadySet &ready, int count) const {
  count = min(count, int(ready.size()));
  vector<int> rids;
  rids.reserve(usize(count));
  for(auto it = ready.begin(); it != ready.end() && rids.size() < usize(count); ++it) {
   rids.push_back(it->rid);
  }
  return rids;
 }
 int plannedTarget(const ReadySet &ready, int target, const vector<int> &legacy) const {
  if(score_.w_tp <= score_.w_c)
   return target;
  return max(target, legacy[ready.size()]);
 }
 PipelinePlan pipelinePlan() const {
  auto bucket = [&](int n) { return usesJointPlan() && active_count_ <= 64 ? n : n ? 1 << (bit_width((unsigned)n) - 1) : 0; };
  int active = bucket(active_count_), decode = bucket(decode_count_);
  if(decode_count_ == active_count_)
   decode += kMaxRequests + 1;
  if(cached_plan_active_ != active || cached_plan_decode_ != decode) {
   cached_plan_ = planKnownDecodeWork();
   cached_plan_active_ = active;
   cached_plan_decode_ = decode;
  }
  return cached_plan_;
 }
 int prefillRemoteCount() const {
  Real edge_work = 0.0L;
  Real uplink_work = 0.0L;
  Real remote_work = 0.0L;
  Real downlink_work = 0.0L;
  int remote_jobs = 0;
  for(const optional<Request> &request : requests_) {
   if(!request || request->finished || !isPrefillStage(request->stage)) {
    continue;
   }
   if(request->stage == READY_P_PRE) {
    edge_work += 2.0L * sys_.schedule_ms + taskTime(prefill_pre_, request->input_len) + taskTime(prefill_post_, request->input_len);
   } else if(request->stage < RUNNING_P_POST) {
    edge_work += sys_.schedule_ms + taskTime(prefill_post_, request->input_len);
   }
   if(request->stage <= WAITING_P_UP) {
    uplink_work += transferTime(request->input_len);
   }
   if(request->stage <= RUNNING_P_PROC) {
    remote_work += sys_.schedule_ms + taskTime(prefill_proc_, request->input_len);
    ++remote_jobs;
   }
   if(request->stage <= WAITING_P_DOWN) {
    downlink_work += transferTime(request->input_len);
   }
  }
  const Real other_bottleneck = max({edge_work, uplink_work, downlink_work});
  if(remote_jobs == 0 || remote_work <= other_bottleneck) {
   return 1;
  }
  if(other_bottleneck <= 0.0L) {
   return min(sys_.remotes, remote_jobs);
  }
  const int required = int(ceil(remote_work / other_bottleneck));
  return clamp(required, 1, min(sys_.remotes, remote_jobs));
 }
 vector<int> decodePreBatch() const {
  if(!usesJointPlan()) {
   return oldestBatch(ready_d_pre_, plannedTarget(ready_d_pre_, pipelinePlan().batch_size, best_pre_batch_));
  }
  const PipelinePlan &plan = pipelinePlan();
  const usize target = min(ready_d_pre_.size(), usize(plan.batch_size));
  vector<int> count(usize(sys_.remotes));
  for(const ReadyKey &key : ready_d_pre_) {
   ++count[usize(requestAt(key.rid)->remote)];
  }
  vector<bool> selected(usize(sys_.remotes), false);
  const int oldest_remote = requestAt(ready_d_pre_.begin()->rid)->remote;
  selected[usize(oldest_remote)] = true;
  vector<int> remotes;
  for(int remote = 0; remote < sys_.remotes; ++remote) {
   if(remote != oldest_remote && count[usize(remote)] > 0) {
    remotes.push_back(remote);
   }
  }
  sort(remotes.begin(), remotes.end(), [&](int lhs, int rhs) { return pair(-count[usize(lhs)], lhs) < pair(-count[usize(rhs)], rhs); });
  int additional = min(plan.active_remotes - 1, int(remotes.size()));
  for(auto it = remotes.begin(); additional; --additional) {
   selected[usize(*it++)] = true;
  }
  vector<int> rids;
  for(const ReadyKey &key : ready_d_pre_) {
   const Request *request = requestAt(key.rid);
   if(selected[usize(request->remote)]) {
    rids.push_back(request->rid);
    if(rids.size() == target) {
     break;
    }
   }
  }
  return rids;
 }
 vector<int> decodeProcBatch(const ReadySet &ready) const {
  if(!usesJointPlan()) {
   return oldestBatch(ready, plannedTarget(ready, pipelinePlan().proc_batch, best_proc_batch_));
  }
  map<int, vector<int>> groups;
  for(const ReadyKey &key : ready) {
   const Request *request = requestAt(key.rid);
   if(!invariant(request != nullptr && request->decode_wave >= 0)) {
    return {};
   }
   groups[request->decode_wave].push_back(request->rid);
  }
  const PipelinePlan &plan = pipelinePlan();
  const usize target = usize((plan.batch_size + plan.active_remotes - 1) / plan.active_remotes);
  vector<int> rids;
  for(const auto &[wave, group] : groups) {
   (void)wave;
   if(!rids.empty() && rids.size() + group.size() > target) {
    break;
   }
   rids.insert(rids.end(), group.begin(), group.end());
   if(rids.size() >= target) {
    break;
   }
  }
  return rids;
 }
 vector<int> completeDecodePostBatch() const {
  vector<int> rids;
  if(!usesJointPlan() || complete_waves_.empty()) {
   return rids;
  }
  const int target = best_post_batch_[complete_members_];
  for(int id : complete_waves_) {
   const auto wave = waves_.find(id);
   if(!invariant(wave != waves_.end()))
    return {};
   if(!rids.empty() && rids.size() + wave->second.rids.size() > usize(target)) {
    break;
   }
   rids.insert(rids.end(), wave->second.rids.begin(), wave->second.rids.end());
   if(rids.size() >= usize(target))
    break;
  }
  return rids;
 }
 bool canFillDecodePost(const vector<int> &joint_post_batch) const {
  vector<bool> pre_remotes(usize(sys_.remotes));
  for(int rid : decodePreBatch()) {
   pre_remotes[usize(requestAt(rid)->remote)] = true;
  }
  const vector<int> post_batch =
   usesJointPlan() ? joint_post_batch : oldestBatch(ready_d_post_, plannedTarget(ready_d_post_, pipelinePlan().post_batch, best_post_batch_));
  return all_of(post_batch.begin(), post_batch.end(), [&](int rid) { return pre_remotes[usize(requestAt(rid)->remote)]; });
 }
 bool batchHasProducedToken(const vector<int> &rids) const {
  return any_of(rids.begin(), rids.end(), [&](int rid) { return requestAt(rid)->has_token; });
 }
 bool shouldAccumulateFirstDecodeWave() const {
  if(ready_d_pre_.empty() || ready_d_pre_.size() >= usize(pipelinePlan().batch_size)) {
   return false;
  }
  if(frontHasProducedToken(ready_d_pre_)) {
   PipelinePlan plan = pipelinePlan();
   auto jobs = knownDecodeJobs();
   int ready = ready_d_pre_.size();
   int depth = clamp(1024 / (int)jobs.size(), 4, 16);
   auto start1 = simulateDecodePlan(jobs, plan.batch_size, plan.proc_batch, plan.post_batch, plan.active_remotes, 1, ready).first;
   auto wait1 = simulateDecodePlan(jobs, plan.batch_size, plan.proc_batch, plan.post_batch, plan.active_remotes, 1, plan.batch_size).first;
   if(wait1 <= start1)
    return false;
   auto startN = simulateDecodePlan(jobs, plan.batch_size, plan.proc_batch, plan.post_batch, plan.active_remotes, depth, ready).first;
   auto waitN = simulateDecodePlan(jobs, plan.batch_size, plan.proc_batch, plan.post_batch, plan.active_remotes, depth, plan.batch_size).first;
   if(waitN <= startN)
    return false;
  }
  Real next_decode_ready = kInfinity;
  for(const optional<Request> &request : requests_) {
   if(request && !request->finished && request->stage != READY_D_PRE) {
    RequestStage stage = request->stage;
    bool guaranteed = stage == RUNNING_P_PRE || stage == WAITING_P_UP || stage == RUNNING_P_PROC || stage == WAITING_P_DOWN || stage == RUNNING_P_POST ||
     stage == RUNNING_D_PRE || stage == WAITING_D_UP || stage == RUNNING_D_PROC || stage == WAITING_D_DOWN || stage == RUNNING_D_POST;
    if(!guaranteed)
     continue;
    Real release = frontierRelease(*request);
    if(release > 0)
     next_decode_ready = min<Real>(next_decode_ready, now_ + release);
   }
  }
  if(!isfinite(next_decode_ready)) {
   return false;
  }
  const vector<int> batch = decodePreBatch();
  const Real task_done = now_ + sys_.schedule_ms + taskTime(decode_pre_, int(batch.size()));
  const Real uplink_done = max<Real>(task_done, up_free_at_) + transferTime(int(batch.size()));
  return next_decode_ready <= uplink_done;
 }
 void createDecodeWave(const vector<int> &rids) {
  const int wave_id = next_wave_++;
  DecodeWave wave{rids, 0};
  for(int rid : rids) {
   Request *request = requestAt(rid);
   (void)invariant(request != nullptr && request->decode_wave == -1);
   request->decode_wave = wave_id;
  }
  waves_.emplace(wave_id, move(wave));
 }
 void markDecodePostReady(int rid) {
  Request *request = requestAt(rid);
  if(request->decode_wave < 0) {
   return;
  }
  auto wave = waves_.find(request->decode_wave);
  if(!invariant(wave != waves_.end() && wave->second.ready_post < int(wave->second.rids.size()))) {
   return;
  }
  ++wave->second.ready_post;
  if(wave->second.ready_post == int(wave->second.rids.size())) {
   complete_waves_.insert(wave->first);
   complete_members_ += wave->second.ready_post;
  }
 }
 void detachDecodePostBatch(const vector<int> &rids) {
  map<int, vector<int>> selected_by_wave;
  for(int rid : rids) {
   Request *request = requestAt(rid);
   if(request->decode_wave >= 0) {
    selected_by_wave[request->decode_wave].push_back(rid);
   }
  }
  for(const auto &[wave_id, selected] : selected_by_wave) {
   auto wave = waves_.find(wave_id);
   if(!invariant(wave != waves_.end() && wave->second.ready_post >= int(selected.size()))) {
    return;
   }
   if(complete_waves_.erase(wave_id) != 0) {
    complete_members_ -= int(wave->second.rids.size());
   }
   set<int> removed(selected.begin(), selected.end());
   vector<int> remaining;
   remaining.reserve(wave->second.rids.size() - selected.size());
   for(int rid : wave->second.rids) {
    if(removed.contains(rid)) {
     requestAt(rid)->decode_wave = -1;
    } else {
     remaining.push_back(rid);
    }
   }
   wave->second.ready_post -= int(selected.size());
   wave->second.rids = move(remaining);
   if(wave->second.rids.empty()) {
    waves_.erase(wave);
   } else if(wave->second.ready_post == int(wave->second.rids.size())) {
    complete_waves_.insert(wave_id);
    complete_members_ += wave->second.ready_post;
   }
  }
 }
 int placementRemoteLimit() const { return usesJointPlan() ? max(pipelinePlan().active_remotes, prefillRemoteCount()) : sys_.remotes; }
 int leastLoadedRemote(int remote_limit) const {
  const int requests_per_remote = (active_count_ + remote_limit - 1) / remote_limit;
  const int decode_batch = best_proc_batch_[usize(requests_per_remote)];
  const Real decode_quantum = (sys_.schedule_ms + taskTime(decode_proc_, decode_batch)) / decode_batch;
  const auto load = [&](int remote) { return prefill_load_[remote] + active_by_remote_[remote] * decode_quantum; };
  const int first = next_remote_ % remote_limit;
  int best = first;
  for(int offset = 1; offset < remote_limit; ++offset) {
   const int remote = (first + offset) % remote_limit;
   if(load(remote) + 1e-12L * max(1.L, abs(load(best))) < load(best)) {
    best = remote;
   }
  }
  return best;
 }
 static bool isPrefillStage(RequestStage stage) { return stage >= READY_P_PRE && stage <= RUNNING_P_POST; }
 Real transferTime(int token_count) const { return sys_.latency_ms + 8.0L * token_count * sys_.token_bytes / (Real(sys_.gbps) * 1.0e6L); }
 Real estimatedPrefillTime(const Request &request) const {
  return 3.0L * sys_.schedule_ms + taskTime(prefill_pre_, request.input_len) + taskTime(prefill_proc_, request.input_len) +
     taskTime(prefill_post_, request.input_len) + 2.0L * transferTime(request.input_len);
 }
 double normalizedLateness(const Request &request) const {
  const bool prefill = isPrefillStage(request.stage);
  if(!prefill && !request.has_token) {
   return -numeric_limits<double>::infinity();
  }
  const double target = prefill ? score_.slo1 : score_.slo2;
  const double reference = prefill ? request.arrival_time : request.decode_ready;
  return (now_ - reference - target) / target;
 }
 bool costlyPrefill(const Request &request) const {
  if(score_.w_c == 0.0) {
   return false;
  }
  const double budget = score_.slo2 / score_.w_c;
  const Real transfer = transferTime(request.input_len);
  const double pre = sys_.schedule_ms + taskTime(prefill_pre_, request.input_len);
  const double proc = sys_.schedule_ms + taskTime(prefill_proc_, request.input_len);
  const double post = sys_.schedule_ms + taskTime(prefill_post_, request.input_len);
  return transfer > budget || pre > budget || proc > budget || post > budget;
 }
 Real decodeProcReadyTime(const Request &request) const {
  const Real edge_ready = edge_task_ ? edge_free_at_ : now_;
  const Real pre_duration = sys_.schedule_ms + min_d_pre_;
  const Real post_duration = sys_.schedule_ms + min_d_post_;
  const auto after_pre = [&](Real start) {
   const Real done = start + pre_duration;
   return max<Real>(done, up_free_at_) + transferTime(1);
  };
  switch(request.stage) {
  case READY_D_PRE:
   return after_pre(edge_ready);
  case RUNNING_D_PRE: {
   int members = 1;
   if(edge_task_) {
    members = int(count_if(edge_task_->rids.begin(), edge_task_->rids.end(), [&](int rid) { return requestAt(rid)->remote == request.remote; }));
   }
   return max<Real>(edge_free_at_, up_free_at_) + transferTime(members);
  }
  case WAITING_D_UP:
   return request.transfer_ready;
  case READY_D_PROC:
  case RUNNING_D_PROC:
   return now_;
  case WAITING_D_DOWN:
   return after_pre(max<Real>(edge_ready, request.transfer_ready) + post_duration);
  case READY_D_POST:
   return after_pre(edge_ready + post_duration);
  case RUNNING_D_POST:
   return after_pre(edge_free_at_);
  default:
   return kInfinity;
  }
 }
 int prefillLayerEnd(const Request &request, int remote) const {
  if(score_.w_c == 0.0 || sys_.layers == 1 || normalizedLateness(request) >= 0.0) {
   return sys_.layers;
  }
  const int remaining = sys_.layers - request.next_layer;
  const Real full_duration = taskTime(prefill_proc_, request.input_len) * remaining / sys_.layers;
  Real next_decode_ready = kInfinity;
  bool active_decode = false;
  for(const optional<Request> &other : requests_) {
   if(!other || other->finished || other->remote != remote || other->stage < READY_D_PRE || other->stage > RUNNING_D_POST) {
    continue;
   }
   active_decode |= other->has_token;
   next_decode_ready = min(next_decode_ready, decodeProcReadyTime(*other));
  }
  if(!isfinite(next_decode_ready) || next_decode_ready >= now_ + sys_.schedule_ms + full_duration) {
   return sys_.layers;
  }
  if(!active_decode) {
   return sys_.layers;
  }
  const Real piece_budget = max<Real>(score_.slo2 / score_.w_c, next_decode_ready - now_);
  if(sys_.schedule_ms + full_duration <= piece_budget) {
   return sys_.layers;
  }
  const Real layer_duration = taskTime(prefill_proc_, request.input_len) / sys_.layers;
  const Real compute_budget = max(0.0L, piece_budget - sys_.schedule_ms);
  const Real affordable_layers = floor(compute_budget / layer_duration);
  const int layers = affordable_layers >= remaining ? remaining : max(1, int(affordable_layers));
  return request.next_layer + layers;
 }
 bool prefillCanWait(const Request &candidate) const {
  if(score_.w_c == 0.0 || normalizedLateness(candidate) >= 0.0 || !costlyPrefill(candidate) ||
     now_ - candidate.arrival_time + estimatedPrefillTime(candidate) >= score_.slo1) {
   return false;
  }
  return true;
 }
 bool shouldDeferPrefill(const Request &candidate) const {
  if(!prefillCanWait(candidate)) {
   return false;
  }
  bool active_decode = false;
  bool guaranteed_event = edge_task_.has_value() || any_of(remote_tasks_.begin(), remote_tasks_.end(), [](const auto &task) { return task.has_value(); });
  for(const optional<Request> &request : requests_) {
   if(!request || request->finished) {
    continue;
   }
   if(request->stage >= READY_D_PRE && request->stage <= RUNNING_D_POST) {
    active_decode |= request->has_token;
   }
   guaranteed_event |= request->stage == WAITING_P_UP || request->stage == WAITING_P_DOWN || request->stage == WAITING_D_UP || request->stage == WAITING_D_DOWN;
  }
  return active_decode && guaranteed_event;
 }
 const Request *mostOverdue(initializer_list<const ReadySet *> ready_sets) const {
  const Request *best = nullptr;
  double best_lateness = -1.0;
  for(const ReadySet *ready : ready_sets) {
   if(ready->empty()) {
    continue;
   }
   const Request *request = requestAt(ready->begin()->rid);
   if(!invariant(request != nullptr)) {
    return nullptr;
   }
   const double lateness = normalizedLateness(*request);
   const double threshold = request->stage == READY_P_PRE && score_.w_c != 0.0 ? max(1.0, score_.dist_base) : 0.0;
   if(lateness < threshold) {
    continue;
   }
   if(best == nullptr || lateness > best_lateness ||
     (lateness == best_lateness && pair(request->arrival_time, request->rid) < pair(best->arrival_time, best->rid))) {
    best = request;
    best_lateness = lateness;
   }
  }
  return best;
 }
 Real batchDrainFinish(usize ready_count, const vector<double> &transfer_times, int target, const TimingCurve &curve, bool wait_for_transfer) const {
  Real time = now_;
  usize next_transfer = 0;
  auto collectTransfers = [&] {
   while(next_transfer < transfer_times.size() && transfer_times[next_transfer] <= time) {
    ++ready_count;
    ++next_transfer;
   }
  };
  if(wait_for_transfer) {
   time = max<Real>(time, transfer_times.front());
   collectTransfers();
  }
  while(ready_count != 0 || next_transfer < transfer_times.size()) {
   if(ready_count == 0) {
    time = max<Real>(time, transfer_times[next_transfer]);
    collectTransfers();
   }
   const int batch = min(target, int(ready_count));
   ready_count -= usize(batch);
   time += sys_.schedule_ms + taskTime(curve, batch);
   collectTransfers();
  }
  return time;
 }
 bool shouldDeferBatch(const ReadySet &ready, int target, const TimingCurve &curve, RequestStage pending_stage, int remote) const {
  if(ready.empty() || (!favorsThroughput() && mostOverdue({&ready}) != nullptr)) {
   return false;
  }
  vector<double> transfer_times;
  for(const optional<Request> &request : requests_) {
   if(request && !request->finished && request->stage == pending_stage && (remote < 0 || request->remote == remote)) {
    transfer_times.push_back(request->transfer_ready);
   }
  }
  if(pending_stage == WAITING_D_UP && edge_task_ && edge_task_->work == DECODE && edge_task_->step == PRE) {
   int count = 0;
   for(int rid : edge_task_->rids)
    count += requestAt(rid)->remote == remote;
   if(count) {
    double ready = max(edge_free_at_, up_free_at_) + transferTime(count);
    for(int i = 0; i < count; ++i)
     transfer_times.push_back(ready);
   }
  }
  if(pending_stage == WAITING_D_DOWN) {
   vector<pair<double, int>> running;
   for(int r = 0; r < sys_.remotes; ++r) {
    if(remote_tasks_[r] && remote_tasks_[r]->work == DECODE && remote_tasks_[r]->step == PROC)
     running.emplace_back(remote_free_at_[r], remote_tasks_[r]->rids.size());
   }
   sort(running.begin(), running.end());
   double link = down_free_at_;
   for(auto [finish, count] : running) {
    link = max(link, finish) + transferTime(count);
    for(int i = 0; i < count; ++i)
     transfer_times.push_back(link);
   }
  }
  if(transfer_times.empty()) {
   return false;
  }
  if(min<usize>(target, ready.size() + transfer_times.size()) <= min<usize>(target, ready.size())) {
   return false;
  }
  sort(transfer_times.begin(), transfer_times.end());
  const Real start_finish = batchDrainFinish(ready.size(), transfer_times, target, curve, false);
  const Real wait_finish = batchDrainFinish(ready.size(), transfer_times, target, curve, true);
  const Real scale = max({1.0L, abs(start_finish), abs(wait_finish)});
  return wait_finish + 1.0e-12L * scale < start_finish;
 }
 static bool atEnd(istringstream &input) {
  input >> ws;
  return input.eof();
 }
 template <class T> static bool parseSingleValue(const string &line, T &value) {
  istringstream input(line);
  return bool(input >> value) && atEnd(input);
 }
 static bool parseServer(const string &token, int &server) {
  if(token == "E") {
   server = -1;
   return true;
  }
  if(token.size() < 2 || token.front() != 'C') {
   return false;
  }
  istringstream input(token.substr(1));
  return bool(input >> server) && server >= 0 && atEnd(input);
 }
 static bool parseCompletedTask(istringstream &input, TaskSpec &task, double &duration) {
  string work;
  string step;
  if(!(input >> work >> step)) {
   return false;
  }
  if(work == "P") {
   task.work = PREFILL;
  } else if(work == "D") {
   task.work = DECODE;
  } else {
   return false;
  }
  if(step == "PRE") {
   task.step = PRE;
  } else if(step == "PROC") {
   task.step = PROC;
  } else if(step == "POST") {
   task.step = POST;
  } else {
   return false;
  }
  if(task.work == PREFILL) {
   int rid = -1;
   if(task.step == PROC) {
    if(!(input >> task.layer_start >> task.layer_end >> task.remote >> rid >> duration)) {
     return false;
    }
   } else if(!(input >> task.remote >> rid >> duration)) {
    return false;
   }
   task.rids.push_back(rid);
  } else {
   int count = 0;
   if(!(input >> task.remote >> count) || count < 1 || count > 2000) {
    return false;
   }
   task.rids.resize(usize(count));
   for(int &rid : task.rids) {
    if(!(input >> rid)) {
     return false;
    }
   }
   if(!(input >> duration)) {
    return false;
   }
  }
  return isfinite(duration) && atEnd(input);
 }
 optional<Event> parseEvent(const string &line) const {
  istringstream input(line);
  string kind;
  if(!(input >> kind)) {
   return nullopt;
  }
  Event event;
  if(kind == "ARR") {
   event.kind = ARRIVAL;
   if(!(input >> event.rid >> event.input_len) || !atEnd(input)) {
    return nullopt;
   }
   return event;
  }
  if(kind == "FIN") {
   event.kind = FINISH;
   if(!(input >> event.rid) || !atEnd(input)) {
    return nullopt;
   }
   return event;
  }
  if(kind == "TDN") {
   event.kind = TASK_DONE;
   string server;
   if(!(input >> server) || !parseServer(server, event.server) || !parseCompletedTask(input, event.task, event.duration)) {
    return nullopt;
   }
   return event;
  }
  if(kind == "XDN") {
   event.kind = TRANSFER_DONE;
   string direction;
   string transfer_kind;
   int count = 0;
   if(!(input >> direction >> event.remote >> event.size >> transfer_kind >> count) || count < 1 || count > 2000) {
    return nullopt;
   }
   if(direction == "UP") {
    event.direction = UP;
   } else if(direction == "DOWN") {
    event.direction = DOWN;
   } else {
    return nullopt;
   }
   if(transfer_kind == "PRE") {
    event.transfer_kind = TransferKind::PREFILL;
   } else if(transfer_kind == "DEC") {
    event.transfer_kind = TransferKind::DECODE;
   } else {
    return nullopt;
   }
   event.rids.resize(usize(count));
   for(int &rid : event.rids) {
    if(!(input >> rid)) {
     return nullopt;
    }
   }
   if(!atEnd(input)) {
    return nullopt;
   }
   return event;
  }
  return nullopt;
 }
 Request *requestAt(int rid) {
  if(rid < 0 || usize(rid) >= requests_.size() || !requests_[usize(rid)]) {
   return nullptr;
  }
  return &*requests_[usize(rid)];
 }
 const Request *requestAt(int rid) const {
  if(rid < 0 || usize(rid) >= requests_.size() || !requests_[usize(rid)]) {
   return nullptr;
  }
  return &*requests_[usize(rid)];
 }
 static ReadyKey readyKey(const Request &request) {
  const bool prefill = isPrefillStage(request.stage);
  const double priority_time = prefill ? request.arrival_time : request.decode_ready;
  return {!prefill && !request.has_token, priority_time, request.rid};
 }
 bool frontHasProducedToken(const ReadySet &ready) const {
  if(ready.empty()) {
   return false;
  }
  const Request *request = requestAt(ready.begin()->rid);
  return invariant(request != nullptr) && request->has_token;
 }
 ReadySet *readySetFor(const Request &request) {
  switch(request.stage) {
  case READY_P_PRE:
   return &ready_p_pre_;
  case READY_P_PROC:
   return &ready_p_proc_[usize(request.remote)];
  case READY_P_POST:
   return &ready_p_post_;
  case READY_D_PRE:
   return &ready_d_pre_;
  case READY_D_PROC:
   return &ready_d_proc_[usize(request.remote)];
  case READY_D_POST:
   return &ready_d_post_;
  default:
   return nullptr;
  }
 }
 void addReady(const Request &request) {
  ReadySet *ready = readySetFor(request);
  if(ready != nullptr) {
   (void)invariant(ready->insert(readyKey(request)).second);
  }
  if(request.stage == READY_P_PRE)
   ready_p_pre_short_.emplace(estimatedPrefillTime(request), request.rid);
 }
 void removeReady(const Request &request) {
  ReadySet *ready = readySetFor(request);
  if(ready != nullptr) {
   (void)invariant(ready->erase(readyKey(request)) == 1);
  }
  if(request.stage == READY_P_PRE)
   ready_p_pre_short_.erase({estimatedPrefillTime(request), request.rid});
 }
 void setStage(Request &request, RequestStage stage) {
  removeReady(request);
  decode_count_ += (stage >= READY_D_PRE && stage <= RUNNING_D_POST) - (request.stage >= READY_D_PRE && request.stage <= RUNNING_D_POST);
  request.stage = stage;
  addReady(request);
 }
 static RequestStage readyStageFor(const TaskSpec &task) {
  if(task.work == PREFILL) {
   if(task.step == PRE) {
    return READY_P_PRE;
   }
   if(task.step == PROC) {
    return READY_P_PROC;
   }
   return READY_P_POST;
  }
  if(task.step == PRE) {
   return READY_D_PRE;
  }
  if(task.step == PROC) {
   return READY_D_PROC;
  }
  return READY_D_POST;
 }
 static RequestStage runningStageFor(const TaskSpec &task) { return RequestStage(int(readyStageFor(task)) + 1); }
 RequestStage completedStageFor(const TaskSpec &task) const {
  if(task.work == PREFILL) {
   if(task.step == PRE) {
    return WAITING_P_UP;
   }
   if(task.step == PROC) {
    return task.layer_end == sys_.layers ? WAITING_P_DOWN : READY_P_PROC;
   }
   return READY_D_PRE;
  }
  if(task.step == PRE) {
   return WAITING_D_UP;
  }
  if(task.step == PROC) {
   return WAITING_D_DOWN;
  }
  return READY_D_PRE;
 }
 int expectedServer(const TaskSpec &task) const { return task.step == PROC ? task.remote : -1; }
 bool validateTaskShape(const TaskSpec &task) const {
  if(!invariant(!task.rids.empty())) {
   return false;
  }
  set<int> unique_ids(task.rids.begin(), task.rids.end());
  if(!invariant(unique_ids.size() == task.rids.size())) {
   return false;
  }
  const bool valid_remote = task.remote >= 0 && task.remote < sys_.remotes;
  if(task.work == PREFILL) {
   if(!invariant(task.rids.size() == 1 && valid_remote)) {
    return false;
   }
   if(task.step == PROC) {
    return invariant(task.layer_start >= 0 && task.layer_start < task.layer_end && task.layer_end <= sys_.layers);
   }
   return invariant(task.layer_start == -1 && task.layer_end == -1);
  }
  if(!invariant(task.layer_start == -1 && task.layer_end == -1)) {
   return false;
  }
  if(task.step == PROC) {
   return invariant(valid_remote);
  }
  return invariant(task.remote == -1);
 }
 bool validateRequests(const TaskSpec &task, RequestStage expected) const {
  for(int rid : task.rids) {
   const Request *request = requestAt(rid);
   if(!invariant(request != nullptr && !request->finished && request->stage == expected)) {
    return false;
   }
   if((task.work == PREFILL || task.step == PROC) && !invariant(request->remote == task.remote)) {
    return false;
   }
   if(task.work == PREFILL && task.step == PROC && !invariant(request->next_layer == task.layer_start)) {
    return false;
   }
  }
  return true;
 }
 bool startAssignment(const Assignment &assignment) {
  if(!validateTaskShape(assignment.task) || !invariant(assignment.server == expectedServer(assignment.task)) ||
     !validateRequests(assignment.task, readyStageFor(assignment.task))) {
   return false;
  }
  if(assignment.server == -1) {
   if(!invariant(!edge_task_)) {
    return false;
   }
  } else {
   const usize remote = usize(assignment.server);
   if(!invariant(!remote_tasks_[remote])) {
    return false;
   }
  }
  if(assignment.task.work == DECODE && assignment.task.step == PRE) {
   createDecodeWave(assignment.task.rids);
  } else if(assignment.task.work == DECODE && assignment.task.step == POST) {
   detachDecodePostBatch(assignment.task.rids);
  }
  if(assignment.server == -1) {
   const int size = assignment.task.work == PREFILL ? requestAt(assignment.task.rids.front())->input_len : int(assignment.task.rids.size());
   const TimingCurve &curve =
    assignment.task.work == PREFILL ? (assignment.task.step == PRE ? prefill_pre_ : prefill_post_) : (assignment.task.step == PRE ? decode_pre_ : decode_post_);
   edge_free_at_ = now_ + sys_.schedule_ms + taskTime(curve, size);
   edge_task_ = assignment.task;
  } else {
   const usize remote = usize(assignment.server);
   remote_tasks_[remote] = assignment.task;
   int size = assignment.task.work == PREFILL ? requestAt(assignment.task.rids.front())->input_len : int(assignment.task.rids.size());
   double duration = taskTime(assignment.task.work == PREFILL ? prefill_proc_ : decode_proc_, size);
   if(assignment.task.work == PREFILL) {
    duration *= (assignment.task.layer_end - assignment.task.layer_start) / double(sys_.layers);
   }
   remote_free_at_[remote] = now_ + sys_.schedule_ms + duration;
  }
  if(assignment.task.work == PREFILL && assignment.task.step == PRE) {
   const int remote = assignment.task.remote;
   ++active_by_remote_[remote];
   prefill_load_[remote] += sys_.schedule_ms + taskTime(prefill_proc_, requestAt(assignment.task.rids[0])->input_len);
  }
  for(int rid : assignment.task.rids) {
   setStage(*requestAt(rid), runningStageFor(assignment.task));
  }
  return true;
 }
 bool applyEvent(const Event &event, double timestamp) {
  if(event.kind == ARRIVAL) {
   return applyArrival(event, timestamp);
  }
  if(event.kind == TASK_DONE) {
   return applyTaskDone(event, timestamp);
  }
  if(event.kind == TRANSFER_DONE) {
   return applyTransferDone(event);
  }
  return false;
 }
 bool applyArrival(const Event &event, double timestamp) {
  if(!invariant(event.rid >= 0 && event.input_len > 0)) {
   return false;
  }
  const usize rid = usize(event.rid);
  if(requests_.size() <= rid) {
   requests_.resize(rid + 1);
  }
  if(!invariant(!requests_[rid])) {
   return false;
  }
  requests_[rid] = Request{event.rid, event.input_len, -1, READY_P_PRE, timestamp, timestamp, false};
  ++active_count_;
  addReady(*requests_[rid]);
  return true;
 }
 void recordTransfer(Direction direction, int token_count, const vector<int> &rids, double timestamp) {
  double &link_free = direction == UP ? up_free_at_ : down_free_at_;
  link_free = double(max<Real>(timestamp, link_free) + transferTime(token_count));
  for(int rid : rids) {
   requestAt(rid)->transfer_ready = link_free;
  }
 }
 void recordTaskTransfers(const TaskSpec &task, double timestamp) {
  if(task.work == PREFILL) {
   const Request *request = requestAt(task.rids.front());
   if(task.step == PRE) {
    recordTransfer(UP, request->input_len, task.rids, timestamp);
   } else if(task.step == PROC && task.layer_end == sys_.layers) {
    recordTransfer(DOWN, request->input_len, task.rids, timestamp);
   }
   return;
  }
  if(task.step == PRE) {
   map<int, vector<int>> by_remote;
   for(int rid : task.rids) {
    by_remote[requestAt(rid)->remote].push_back(rid);
   }
   for(const auto &[remote, rids] : by_remote) {
    (void)remote;
    recordTransfer(UP, int(rids.size()), rids, timestamp);
   }
  } else if(task.step == PROC) {
   recordTransfer(DOWN, int(task.rids.size()), task.rids, timestamp);
  }
 }
 bool applyTaskDone(const Event &event, double timestamp) {
  if(!validateTaskShape(event.task) || !invariant(event.server == expectedServer(event.task)) || !validateRequests(event.task, runningStageFor(event.task))) {
   return false;
  }
  if(event.server == -1) {
   if(!invariant(edge_task_ && *edge_task_ == event.task)) {
    return false;
   }
   edge_task_.reset();
  } else {
   const usize remote = usize(event.server);
   if(!invariant(remote < remote_tasks_.size() && remote_tasks_[remote] && *remote_tasks_[remote] == event.task)) {
    return false;
   }
   remote_tasks_[remote].reset();
  }
  recordTaskTransfers(event.task, timestamp);
  const RequestStage next = completedStageFor(event.task);
  for(int rid : event.task.rids) {
   Request *request = requestAt(rid);
   if(event.task.work == PREFILL && event.task.step == PROC) {
    prefill_load_[event.task.remote] -= taskTime(prefill_proc_, request->input_len) * (event.task.layer_end - event.task.layer_start) / sys_.layers +
     (event.task.layer_end == sys_.layers ? sys_.schedule_ms : 0);
    request->next_layer = event.task.layer_end;
    if(event.task.layer_end < sys_.layers) {
     yield_to_decode_[usize(event.task.remote)] = true;
    }
   }
   if(event.task.step == POST) {
    request->decode_ready = timestamp;
    request->has_token |= event.task.work == DECODE;
   }
   setStage(*request, next);
  }
  return true;
 }
 bool applyTransferDone(const Event &event) {
  if(!invariant(event.remote >= 0 && event.remote < sys_.remotes && event.size >= 0)) {
   return false;
  }
  set<int> unique_ids(event.rids.begin(), event.rids.end());
  if(!invariant(!event.rids.empty() && unique_ids.size() == event.rids.size())) {
   return false;
  }
  RequestStage expected;
  RequestStage next;
  int64_t expected_size = 0;
  if(event.transfer_kind == TransferKind::PREFILL) {
   if(!invariant(event.rids.size() == 1)) {
    return false;
   }
   const Request *request = requestAt(event.rids.front());
   if(!invariant(request != nullptr)) {
    return false;
   }
   expected_size = int64_t(request->input_len) * sys_.token_bytes;
   if(event.direction == UP) {
    expected = WAITING_P_UP;
    next = READY_P_PROC;
   } else {
    expected = WAITING_P_DOWN;
    next = READY_P_POST;
   }
  } else {
   expected_size = int64_t(event.rids.size()) * sys_.token_bytes;
   if(event.direction == UP) {
    expected = WAITING_D_UP;
    next = READY_D_PROC;
   } else {
    expected = WAITING_D_DOWN;
    next = READY_D_POST;
   }
  }
  if(!invariant(event.size == expected_size)) {
   return false;
  }
  for(int rid : event.rids) {
   const Request *request = requestAt(rid);
   if(!invariant(request != nullptr && !request->finished && request->remote == event.remote && request->stage == expected)) {
    return false;
   }
  }
  for(int rid : event.rids) {
   setStage(*requestAt(rid), next);
   requestAt(rid)->transfer_ready = 0.0;
   if(next == READY_D_POST) {
    markDecodePostReady(rid);
   }
  }
  return true;
 }
 bool applyFinish(int rid) {
  Request *request = requestAt(rid);
  if(!invariant(request != nullptr && !request->finished && request->stage == READY_D_PRE)) {
   return false;
  }
  setStage(*request, FINISHED);
  request->finished = true;
  --active_by_remote_[request->remote];
  --active_count_;
  return true;
 }
 void writeTaskSpec(const TaskSpec &task) {
  output_ << (task.work == PREFILL ? 'P' : 'D') << ' ';
  if(task.step == PRE) {
   output_ << "PRE";
  } else if(task.step == PROC) {
   output_ << "PROC";
  } else {
   output_ << "POST";
  }
  if(task.work == PREFILL && task.step == PROC) {
   output_ << ' ' << task.layer_start << ' ' << task.layer_end;
  }
  output_ << ' ' << task.remote;
  if(task.work == DECODE) {
   output_ << ' ' << task.rids.size();
  }
  for(int rid : task.rids) {
   output_ << ' ' << rid;
  }
 }
 istream &input_;
 ostream &output_;
 SystemConfig sys_;
 ScoringConfig score_;
 TimingCurve prefill_pre_, prefill_proc_, prefill_post_;
 TimingCurve decode_pre_, decode_proc_, decode_post_;
 vector<int> best_pre_batch_, best_proc_batch_, best_post_batch_;
 double min_d_pre_ = 0.0, min_d_post_ = 0.0;
 vector<optional<Request>> requests_;
 int active_count_ = 0, decode_count_ = 0;
 double now_ = 0.0, up_free_at_ = 0.0, down_free_at_ = 0.0, edge_free_at_ = 0.0;
 vector<bool> yield_to_decode_;
 bool deferred_d_pre_ = false;
 optional<TaskSpec> edge_task_;
 vector<optional<TaskSpec>> remote_tasks_;
 vector<double> remote_free_at_;
 vector<int> active_by_remote_;
 vector<Real> prefill_load_;
 int next_remote_ = 0, next_wave_ = 0;
 map<int, DecodeWave> waves_;
 set<int> complete_waves_;
 int complete_members_ = 0;
 ReadySet ready_p_pre_, ready_p_post_, ready_d_pre_, ready_d_post_;
 set<pair<Real, int>> ready_p_pre_short_;
 vector<ReadySet> ready_p_proc_;
 vector<ReadySet> ready_d_proc_;
 mutable PipelinePlan cached_plan_;
 mutable int cached_plan_active_ = -1, cached_plan_decode_ = -1;
};
int main() {
 ios::sync_with_stdio(false);
 cin.tie(nullptr);
 Scheduler scheduler(cin, cout);
 if(!scheduler.readStartup()) {
  return 0;
 }
 while(scheduler.readAndProcessFrame()) {
 }
 return 0;
}
