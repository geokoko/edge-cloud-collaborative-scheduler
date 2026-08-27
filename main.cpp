#include <bits/stdc++.h>
#include <cassert>

using usize = std::size_t;
using Real = long double;
constexpr Real kInfinity = std::numeric_limits<Real>::infinity();

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
  int server = -1;
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
  int proc_batch = 1;
  int post_batch = 1;
  int active_remotes = 1;
  Real quality = -kInfinity;
  Real finish = kInfinity;
};

struct DecodeWave {
  std::vector<int> request_ids;
  int ready_post = 0;
};

class Scheduler {
public:
Scheduler(std::istream&input,std::ostream&output)
:input_(input),output_(output){}bool readStartup(){
if(!(input_>>system_.remote_count>>system_.schedule_cost>>
system_.latency_in_ms>>system_.bandwidth_gbps>>
system_.bytes_per_token>>system_.num_layers)){
return false;}if(!(input_>>scoring_.slo1>>scoring_.slo2>>
scoring_.throughput_upper_bound>>scoring_.throughput_base>>
scoring_.distance_base>>scoring_.throughput_weight>>
scoring_.waiting_weight)){
return false;}int row_count=0;if(!(input_>>row_count)||row_count<0){
return false;}for(int row=0;row<row_count;++row){
int batch_size=0;double prefill_pre=0.0;double prefill_proc=0.0;double prefill_post=0.0;double decode_pre=0.0;double decode_proc=0.0;double decode_post=0.0;if(!(input_>>batch_size>>prefill_pre>>prefill_proc>>
 prefill_post>>decode_pre>>decode_proc>>decode_post)){
return false;}if(prefill_pre>=0.0){
prefill_pre_.emplace_back(batch_size,prefill_pre);}if(prefill_proc>=0.0){
prefill_proc_.emplace_back(batch_size,prefill_proc);}if(prefill_post>=0.0){
prefill_post_.emplace_back(batch_size,prefill_post);}if(decode_pre>=0.0){
decode_pre_.emplace_back(batch_size,decode_pre);}if(decode_proc>=0.0){
decode_proc_.emplace_back(batch_size,decode_proc);}if(decode_post>=0.0){
decode_post_.emplace_back(batch_size,decode_post);}}if(system_.remote_count<=0||system_.num_layers<=0||
system_.bytes_per_token<=0||scoring_.slo1<=0.0||
scoring_.slo2<=0.0){
return false;}if(!prepareSchedulingData()){
return false;}yield_to_decode_.assign(usize(system_.remote_count),
      false);remote_tasks_.resize(usize(system_.remote_count));remote_task_finish_time_.assign(system_.remote_count,0);ready_p_proc_.resize(usize(system_.remote_count));ready_d_proc_.resize(usize(system_.remote_count));active_by_remote_.assign(system_.remote_count,0);prefill_load_.assign(system_.remote_count,0);input_.ignore(std::numeric_limits<std::streamsize>::max(),'\n');return true;}bool readAndProcessFrame(){
std::string header;if(!std::getline(input_,header)||header=="END"){
return false;}double timestamp=0.0;if(!parseSingleValue(header,timestamp)||!std::isfinite(timestamp)){
return false;}std::string count_line;int event_count=0;if(!std::getline(input_,count_line)||
!parseSingleValue(count_line,event_count)||event_count<0){
return false;}std::vector<Event>events;events.reserve(usize(event_count));for(int i=0;i<event_count;++i){
std::string line;if(!std::getline(input_,line)){
return false;}std::optional<Event>event=parseEvent(line);if(!event){
return false;}events.push_back(std::move(*event));}current_time_=timestamp;for(const Event&event:events){
if(event.kind!=EventKind::FINISH&&!applyEvent(event,timestamp)){
return false;}}for(const Event&event:events){
if(event.kind==EventKind::FINISH&&!applyFinish(event.rid)){
return false;}}
std::vector<Assignment>assignments=chooseAssignments();
emitAssignments(assignments);
return static_cast<bool>(output_);
}

std::vector<Assignment> chooseAssignments() {
  std::vector<Assignment> assignments;
  assignments.reserve(usize(system_.remote_count+1));

  for(int remote=0;remote<system_.remote_count;++remote) {
    if(remote_tasks_[usize(remote)]) continue;

    TaskSpec task;
    bool found=false;
    const ReadySet&decode=ready_d_proc_[usize(remote)];
    const ReadySet&prefill=ready_p_proc_[usize(remote)];
    const bool active_decode=frontHasProducedToken(decode);
    const int proc_target=decode.empty()?1:plannedTarget(
      decode,currentPipelinePlan().proc_batch,best_decode_proc_batch_);
    const bool defer_decode=!usesJointDecodePlan()&&!decode.empty()&&
      shouldDeferBatch(decode,proc_target,decode_proc_,
                       RequestStage::WAITING_D_UP,remote);

    if(yield_to_decode_[usize(remote)]&&!decode.empty()) {
      task={WorkKind::DECODE,TaskStep::PROC,remote,-1,-1,
            decodeProcBatch(decode)};
      found=true;
    } else {
      const Request*overdue=mostOverdue({&decode,&prefill});
      if(overdue&&overdue->stage==RequestStage::READY_P_PROC) {
        int layer_end=system_.num_layers;
        if(active_decode&&overdue->next_prefill_layer==0&&
           system_.num_layers>1) {
          layer_end=(system_.num_layers+1)/2;
        }
        task={WorkKind::PREFILL,TaskStep::PROC,remote,
              overdue->next_prefill_layer,layer_end,{overdue->rid}};
        found=true;
      } else if(!decode.empty()&&(active_decode||prefill.empty())&&
                !defer_decode) {
        task={WorkKind::DECODE,TaskStep::PROC,remote,-1,-1,
              decodeProcBatch(decode)};
        found=true;
      } else if(!prefill.empty()) {
        const Request*request=requestAt(prefill.begin()->rid);
        if(!invariant(request!=nullptr)) return assignments;
        int layer_end=prefillLayerEnd(*request,remote);
        const bool hold_down=scoring_.waiting_weight>0.0&&
          normalizedLateness(*request)<0.0&&
          transferTime(request->input_length)>
            scoring_.slo2/scoring_.waiting_weight&&
          std::any_of(requests_.begin(),requests_.end(),
            [](const std::optional<Request>&other) {
              return other&&!other->finished&&other->has_produced_token&&
                other->stage>=RequestStage::READY_D_PRE&&
                other->stage<=RequestStage::RUNNING_D_POST;
            });
        if(hold_down&&layer_end==system_.num_layers) --layer_end;
        if(layer_end==request->next_prefill_layer) continue;
        task={WorkKind::PREFILL,TaskStep::PROC,remote,
              request->next_prefill_layer,layer_end,{request->rid}};
        found=true;
      }
    }

    if(found) {
      Assignment assignment{remote,std::move(task)};
      if(!startAssignment(assignment)) return assignments;
      yield_to_decode_[usize(remote)]=false;
      assignments.push_back(std::move(assignment));
    }
  }

  if(!edge_task_) {
    TaskSpec task;
    const Request*overdue=mostOverdue(
      {&ready_d_post_,&ready_p_post_,&ready_d_pre_,&ready_p_pre_});
    std::vector<int> decode_post_batch;
    if(usesJointDecodePlan()) {
      if(overdue&&overdue->stage==RequestStage::READY_D_POST) {
        const auto wave=decode_waves_.find(overdue->decode_wave);
        if(wave!=decode_waves_.end()&&
           complete_decode_waves_.contains(wave->first)) {
          decode_post_batch=wave->second.request_ids;
        } else {
          decode_post_batch={overdue->rid};
        }
      } else {
        decode_post_batch=completeDecodePostBatch();
      }
    }
    const bool decode_post_available=
usesJointDecodePlan()?!decode_post_batch.empty()
:!ready_d_post_.empty();const int post_target=ready_d_post_.empty()?1:plannedTarget(ready_d_post_,
currentPipelinePlan().post_batch,best_decode_post_batch_);const bool defer_decode_post=!usesJointDecodePlan()&&decode_post_available&&(
shouldDeferBatch(ready_d_post_,post_target,
decode_post_,RequestStage::WAITING_D_DOWN,-1)||
(ready_d_post_.size()<usize(
post_target)&&
(!ready_d_pre_.empty()||!ready_p_post_.empty())&&
(favorsThroughput()||
mostOverdue({&ready_d_post_})==nullptr)));const bool decode_post_schedulable=decode_post_available&&!defer_decode_post;const bool active_d_post=
decode_post_schedulable&&(usesJointDecodePlan()
?batchHasProducedToken(decode_post_batch)
:frontHasProducedToken(ready_d_post_));const bool active_d_pre=frontHasProducedToken(ready_d_pre_);const int fill_target=active_d_pre?currentPipelinePlan().batch_size:1;const bool fill_decode_pre=!deferred_d_pre_&&active_d_pre&&
scoring_.throughput_weight>=3.0*scoring_.waiting_weight&&
ready_d_pre_.size()<usize(
fill_target);std::optional<RequestStage>selected_stage;if(overdue!=nullptr&&!(overdue->stage==RequestStage::READY_D_POST&&
defer_decode_post)){
selected_stage=overdue->stage;}else if(active_d_post){
selected_stage=RequestStage::READY_D_POST;}else if(fill_decode_pre&&!ready_p_post_.empty()){
selected_stage=RequestStage::READY_P_POST;}else if(fill_decode_pre&&decode_post_schedulable&&
canFillDecodePost(decode_post_batch)){
selected_stage=RequestStage::READY_D_POST;}else if(active_d_pre){
selected_stage=RequestStage::READY_D_PRE;}else if(!ready_p_post_.empty()){
selected_stage=RequestStage::READY_P_POST;}else if(!ready_p_pre_.empty()){
selected_stage=RequestStage::READY_P_PRE;}else if(decode_post_schedulable){
selected_stage=RequestStage::READY_D_POST;}else if(!ready_d_pre_.empty()){
selected_stage=RequestStage::READY_D_PRE;}if(selected_stage==RequestStage::READY_D_PRE&&
shouldAccumulateFirstDecodeWave()){
if(!ready_p_post_.empty())selected_stage=RequestStage::READY_P_POST;else if(!ready_p_pre_.empty())selected_stage=RequestStage::READY_P_PRE;else selected_stage.reset();}if(selected_stage==RequestStage::READY_D_POST){
if(usesJointDecodePlan()){
if(!invariant(!decode_post_batch.empty())){
  return assignments;}  task={WorkKind::DECODE,TaskStep::POST,-1,-1,-1,
    std::move(decode_post_batch)};}else{
task={WorkKind::DECODE,TaskStep::POST,-1,-1,-1,
    oldestBatch(ready_d_post_,post_target)};}}else if(selected_stage==RequestStage::READY_P_POST){
const int rid=ready_p_post_.begin()->rid;task={WorkKind::PREFILL,TaskStep::POST,
  requestAt(rid)->remote,-1,-1,{rid}};}else if(selected_stage==RequestStage::READY_D_PRE){
task={WorkKind::DECODE,TaskStep::PRE,-1,-1,-1,
  decodePreBatch()};}else if(selected_stage==RequestStage::READY_P_PRE){
const Request*candidate=
overdue!=nullptr&&
    overdue->stage==RequestStage::READY_P_PRE&&
    scoring_.waiting_weight&&
    normalizedLateness(*overdue)>=
      std::max(1.0,scoring_.distance_base)
?overdue
:shortestPrefill(ready_p_pre_);if(!invariant(candidate!=nullptr)){
return assignments;}const int rid=candidate->rid;Request*request=requestAt(rid);if(!invariant(request!=nullptr&&request->remote==-1)){
return assignments;}if(shouldDeferPrefill(*request)){
selected_stage.reset();}else{
const int remote_limit=placementRemoteLimit();  request->remote=leastLoadedRemote(remote_limit);  next_remote_=(request->remote+1)%remote_limit;  task={WorkKind::PREFILL,TaskStep::PRE,request->remote,
    -1,-1,{rid}};}}
    if(selected_stage) {
      Assignment assignment{-1,std::move(task)};
      if(!startAssignment(assignment)) return assignments;
      if(assignment.task.work==WorkKind::DECODE&&
         assignment.task.step==TaskStep::PRE) deferred_d_pre_=false;
      else if(active_d_pre) deferred_d_pre_=true;
      assignments.push_back(std::move(assignment));
    }
  }
  return assignments;
}

void emitAssignments(const std::vector<Assignment>&assignments) {
  output_<<assignments.size()<<'\n';
  for(const Assignment&assignment:assignments) {
    if(assignment.server==-1) output_<<'E';
    else output_<<'C'<<assignment.server;
    output_<<' ';
    writeTaskSpec(assignment.task);
    output_<<'\n';
  }
  output_<<std::flush;
}

private:
using ReadySet=std::set<ReadyKey>;
using TimingCurve=std::vector<std::pair<int,double>>;
static constexpr int kMaxRequests=2000;
static bool invariant(bool condition){
assert(condition);return condition;}static double lookupTime(const TimingCurve&curve,int batch_size){
const auto upper=std::lower_bound(
curve.begin(),curve.end(),batch_size,
[](const auto&point,int size){return point.first<size;});if(upper==curve.begin()){
return upper->second;}if(upper==curve.end()){
return curve.back().second;}if(upper->first==batch_size){
return upper->second;}const auto lower=std::prev(upper);const double position=static_cast<double>(batch_size - lower->first)/
(upper->first - lower->first);return lower->second+position*(upper->second - lower->second);}std::vector<int>bestBatchSizes(const TimingCurve&curve)const{
std::vector<int>choices(kMaxRequests+1,1);Real best_rate=-1.0L;int best_size=1;for(int size=1;size<=kMaxRequests;++size){
const Real rate=
size/(Real(system_.schedule_cost)+
  lookupTime(curve,size));if(rate>best_rate){
best_rate=rate;best_size=size;}choices[usize(size)]=best_size;}return choices;}bool usesJointDecodePlan()const{
return scoring_.throughput_weight>scoring_.waiting_weight&&
system_.remote_count>1&&system_.latency_in_ms>=
system_.schedule_cost+lookupTime(decode_proc_,1);}bool favorsThroughput()const{
return scoring_.waiting_weight==0||(scoring_.distance_base>0&&
scoring_.waiting_weight/scoring_.distance_base<scoring_.throughput_weight);}Real frontierRelease(const Request&r)const{
Real edge=edge_task_?std::max(0.L,(Real)edge_task_finish_time_-
current_time_):0;Real remote=r.remote>=0&&remote_tasks_[r.remote]?
std::max(0.L,(Real)remote_task_finish_time_[r.remote]-current_time_):0;Real post=system_.schedule_cost+lookupTime(prefill_post_,r.input_length);Real proc=system_.schedule_cost+lookupTime(prefill_proc_,r.input_length)*
(system_.num_layers-r.next_prefill_layer)/system_.num_layers;Real transfer=transferTime(r.input_length);Real dpost=system_.schedule_cost+lookupTime(decode_post_,1);Real dproc=system_.schedule_cost+lookupTime(decode_proc_,1);Real dtransfer=transferTime(1);switch(r.stage){
case RequestStage::READY_P_PRE:return edge+system_.schedule_cost+
lookupTime(prefill_pre_,r.input_length)+2*transfer+proc+post;case RequestStage::RUNNING_P_PRE:return std::max(0.L,
(Real)edge_task_finish_time_-current_time_)+2*transfer+proc+post;case RequestStage::WAITING_P_UP:return std::max(0.L,
(Real)r.transfer_ready_time-current_time_)+proc+transfer+post;case RequestStage::READY_P_PROC:return remote+proc+transfer+post;case RequestStage::RUNNING_P_PROC:return std::max(0.L,
(Real)remote_task_finish_time_[r.remote]-current_time_)+transfer+post;case RequestStage::WAITING_P_DOWN:return std::max(0.L,
(Real)r.transfer_ready_time-current_time_)+post;case RequestStage::READY_P_POST:return edge+post;case RequestStage::RUNNING_P_POST:return std::max(0.L,
(Real)edge_task_finish_time_-current_time_);case RequestStage::READY_D_PRE:return 0;case RequestStage::RUNNING_D_PRE:return std::max(0.L,
(Real)edge_task_finish_time_-current_time_)+
dtransfer+dproc+dtransfer+dpost;case RequestStage::WAITING_D_UP:return std::max(0.L,
(Real)r.transfer_ready_time-current_time_)+dproc+dtransfer+dpost;case RequestStage::READY_D_PROC:return remote+dproc+dtransfer+dpost;case RequestStage::RUNNING_D_PROC:return std::max(0.L,
(Real)remote_task_finish_time_[r.remote]-current_time_)+
dtransfer+dpost;case RequestStage::WAITING_D_DOWN:return std::max(0.L,
(Real)r.transfer_ready_time-current_time_)+dpost;case RequestStage::READY_D_POST:return edge+dpost;case RequestStage::RUNNING_D_POST:return std::max(0.L,
(Real)edge_task_finish_time_-current_time_);default:return 0;}
}

// Compare a candidate plan by simulating only currently known decode work.
std::pair<Real,Real> simulateDecodePlan(
  std::vector<std::pair<Real,int>>jobs,int pre_batch,int proc_batch,
  int post_batch,int remote_limit,int cycles,int first_batch=0) const {
const bool joint=usesJointDecodePlan();struct Item{int order,generation,remote,wave,job;};struct Group{std::vector<Item>items;};std::array<int,8>placement{};for(auto[release,remote]:jobs)if(remote>=0)++placement[remote];for(auto&job:jobs)if(job.second<0){
int best=0;for(int r=1;r<remote_limit;++r){if(placement[r]<placement[best])best=r;}job.second=best;++placement[best];}
std::vector<Group>groups;using Ev=std::tuple<Real,int,int,int,int>;std::priority_queue<Ev,std::vector<Ev>,std::greater<Ev>>events;std::array<std::deque<Item>,8>pre_ready,proc_ready;std::deque<Item>post_ready;std::deque<int>post_waves;std::vector<int>wave_total,wave_down;std::vector<std::vector<Item>>wave_items;std::array<bool,8>remote_busy{};bool edge_busy=edge_task_.has_value();int sequence=0,ready_pre=0,remaining_pre=jobs.size(),remaining_post=0,
done_members=0,next_active=-1;std::array<int,8>remaining_proc{};for(auto job:jobs)++remaining_proc[job.second];const int members=jobs.size()*cycles;bool first_wave=true;Real up=std::max(0.L,(Real)uplink_free_time_-current_time_);Real down=std::max(0.L,(Real)downlink_free_time_-current_time_);Real gap_sum=0,completion_sum=0,last=0,now=0;int gaps=0;std::vector<Real>last_token(jobs.size(),-1);if(edge_busy){events.emplace(std::max(0.L,(Real)edge_task_finish_time_-
current_time_),sequence++,5,-1,-1);}for(int r=0;r<system_.remote_count;++r)if(remote_tasks_[r]){
remote_busy[r]=true;events.emplace(std::max(0.L,(Real)remote_task_finish_time_[r]-
current_time_),sequence++,6,-1,r);}
for(int job=0;job<(int)jobs.size();++job){
events.emplace(jobs[job].first,sequence++,7,job,jobs[job].second);}auto save=[&](std::vector<Item>&&items){
groups.push_back({std::move(items)});return static_cast<int>(groups.size()-1);};auto dispatch=[&](Real time){
for(int r=0;r<system_.remote_count;++r)if(!remote_busy[r]&&!proc_ready[r].empty()){
int ready=proc_ready[r].size();int size=std::min(ready,proc_batch);if(joint){
size=0;while(size<ready){int end=size+1;while(end<ready&&proc_ready[r][end].wave==proc_ready[r][size].wave)++end;if(size&&end>proc_batch)break;size=end;if(size>=proc_batch)break;}}else if(ready<proc_batch&&ready<remaining_proc[r])continue;std::vector<Item>items;for(int i=0;i<size;++i){items.push_back(proc_ready[r].front());proc_ready[r].pop_front();}remaining_proc[r]-=size;remaining_post+=size;int group=save(std::move(items));remote_busy[r]=true;events.emplace(time+system_.schedule_cost+lookupTime(decode_proc_,size),
sequence++,2,group,r);}
if(edge_busy)return;
int post_count=post_ready.size();if(joint&&!post_waves.empty()){
std::vector<Item>items;do{
int wave=post_waves.front();  if(!items.empty()&&items.size()+wave_items[wave].size()>
  usize(post_batch)){break;}  post_waves.pop_front();  items.insert(items.end(),wave_items[wave].begin(),wave_items[wave].end());}while(!post_waves.empty());remaining_post-=items.size();int size=items.size(),group=save(std::move(items));edge_busy=true;events.emplace(time+system_.schedule_cost+
lookupTime(decode_post_,size),sequence++,4,group,-1);}else if(!joint&&post_count&&
(post_count>=post_batch||post_count==remaining_post)){
int size=std::min(post_count,post_batch);std::vector<Item>items;for(int i=0;i<size;++i){items.push_back(post_ready.front());post_ready.pop_front();}remaining_post-=size;int group=save(std::move(items));edge_busy=true;events.emplace(time+system_.schedule_cost+
lookupTime(decode_post_,size),sequence++,4,group,-1);}else if(ready_pre){
int target=first_wave&&first_batch?first_batch:pre_batch;if(ready_pre<target&&ready_pre<remaining_pre)return;int oldest=-1;for(int r=0;r<system_.remote_count;++r){if(!pre_ready[r].empty()&&
(oldest<0||pre_ready[r].front().order<pre_ready[oldest].front().order))
oldest=r;}std::vector<int>remotes{oldest};while((int)remotes.size()<remote_limit){
int best=-1;  for(int r=0;r<system_.remote_count;++r){if(!pre_ready[r].empty()&&
  std::find(remotes.begin(),remotes.end(),r)==remotes.end()&&
(best<0||std::pair(-static_cast<int>(pre_ready[r].size()),r)<
    std::pair(-static_cast<int>(pre_ready[best].size()),best)))best=r;}  if(best<0)break;  remotes.push_back(best);}std::vector<Item>items;while((int)items.size()<target){
int remote=-1;  for(int r:remotes){if(!pre_ready[r].empty()&&
(remote<0||pre_ready[r].front().order<pre_ready[remote].front().order))
  remote=r;}  if(remote<0)break;  items.push_back(pre_ready[remote].front());pre_ready[remote].pop_front();--ready_pre;}remaining_pre-=items.size();int size=items.size(),wave=wave_total.size();for(Item&item:items)item.wave=wave;wave_total.push_back(size);wave_down.push_back(0);wave_items.push_back(items);int group=save(std::move(items));edge_busy=true;first_wave=false;events.emplace(time+system_.schedule_cost+
lookupTime(decode_pre_,size),sequence++,0,group,-1);}};std::vector<Ev>frame;dispatch(0);while(done_members<members){
if(events.empty()){return{kInfinity,
kInfinity};}now=std::get<0>(events.top());frame.clear();while(!events.empty()&&std::get<0>(events.top())==now){
frame.push_back(events.top());events.pop();}for(auto[time,seq,type,group,remote]:frame){
static_cast<void>(seq);if(type==0){
edge_busy=false;  std::array<std::vector<Item>,8>split;  for(Item item:groups[group].items)split[item.remote].push_back(item);  for(int r=0;r<system_.remote_count;++r)if(!split[r].empty()){
  int child=save(std::move(split[r]));    up=std::max(up,time)+transferTime(groups[child].items.size());    events.emplace(up,sequence++,1,child,r);}}else if(type==1){for(Item item:groups[group].items)
proc_ready[remote].push_back(item);}else if(type==2){
remote_busy[remote]=false;  down=std::max(down,time)+transferTime(groups[group].items.size());  events.emplace(down,sequence++,3,group,remote);}else if(type==3){
if(joint){
  int wave=groups[group].items.front().wave;    wave_down[wave]+=groups[group].items.size();    if(wave_down[wave]==wave_total[wave])post_waves.push_back(wave);}else for(Item item:groups[group].items)post_ready.push_back(item);}else if(type==4){
edge_busy=false;int count=groups[group].items.size();  done_members+=count;last=time;completion_sum+=time*count;  for(Item item:groups[group].items){
  if(last_token[item.job]>=0){gap_sum+=time-last_token[item.job];++gaps;}    last_token[item.job]=time;}  for(auto it=groups[group].items.rbegin();it!=groups[group].items.rend();++it)
if(it->generation+1<cycles){
  Item item{next_active--,it->generation+1,it->remote,-1,it->job};    pre_ready[item.remote].push_front(item);++ready_pre;++remaining_pre;++remaining_proc[item.remote];}}else if(type==5)edge_busy=false;else if(type==6)remote_busy[remote]=false;else{
Item item{group,0,remote,-1,group};pre_ready[remote].push_back(item);++ready_pre;}}dispatch(now);}Real tp=members/std::max(last,1e-12L);Real norm_tp=std::clamp((tp-scoring_.throughput_base)/
(scoring_.throughput_upper_bound-scoring_.throughput_base),0.L,1.L);Real mean=joint?completion_sum/members:(gaps?gap_sum/gaps:0);Real excess=std::max(0.L,mean/scoring_.slo2-1);Real norm_c=scoring_.distance_base>0?
std::max(0.L,1-(joint?1.5L:1.L)*excess/scoring_.distance_base):
(excess==0?1:0);
return {scoring_.throughput_weight*norm_tp+
        scoring_.waiting_weight*norm_c,last};
}

std::vector<std::pair<Real,int>> knownDecodeJobs(int requested=256) const {
  std::vector<std::pair<Real,int>> jobs;
  for(bool active:{true,false}) {
    for(const auto&request:requests_) {
      if(request&&!request->finished&&request->has_produced_token==active) {
        jobs.emplace_back(frontierRelease(*request),request->remote);
      }
    }
  }
  std::stable_sort(jobs.begin(),jobs.end(),
                   [](auto&a,auto&b){return a.first<b.first;});
  if(requested<(int)jobs.size()) jobs.resize(requested);
  return jobs;
}

PipelinePlan planKnownDecodeWork(int requested=256,int cycles=0) const {
  auto jobs=knownDecodeJobs(requested);
  int total=std::max((int)jobs.size(),1);
  if(!cycles) {
    bool strong=usesJointDecodePlan()&&
                scoring_.throughput_weight>3*scoring_.waiting_weight;
    cycles=std::clamp((strong?1280:1024)/total,4,16);
  }

  std::set<int> sizes{
    1,total,(total+1)/2,best_decode_pre_batch_[total],
    best_decode_proc_batch_[total],best_decode_post_batch_[total]
  };
  for(int groups=1;groups<=std::min(total,12);++groups) {
    sizes.insert((total+groups-1)/groups);
    sizes.insert(total/groups);
  }
  for(int size=1;size<total;size*=2) sizes.insert(size);

  PipelinePlan answer;
  auto better=[&](Real quality,Real finish) {
    if(!answer.finish||
       scoring_.throughput_weight>3*scoring_.waiting_weight) {
      return !answer.finish||finish<answer.finish;
    }
    Real scale=std::max({1.L,std::abs(quality),std::abs(answer.quality)});
    return quality>answer.quality+1e-12L*scale||
           (std::abs(quality-answer.quality)<=1e-12L*scale&&
            finish<answer.finish);
  };

  for(int remotes=1;
      remotes<=std::min(system_.remote_count,total);++remotes) {
    for(int size:sizes) {
      if(size<=0||size>total) continue;
      int proc=(size+remotes-1)/remotes;
      auto[quality,finish]=simulateDecodePlan(
        jobs,size,proc,size,remotes,cycles);
      if(better(quality,finish)) {
        answer={size,proc,size,remotes,quality,finish};
      }
    }
  }

  if(!usesJointDecodePlan()) {
    std::set<int> proc_sizes{1,best_decode_proc_batch_[total]};
    for(int size:sizes) {
      proc_sizes.insert((size+answer.active_remotes-1)/
                        answer.active_remotes);
    }
    bool changed=false;
    auto improve=[&](int pre,int proc,int post) {
      if(pre==answer.batch_size&&proc==answer.proc_batch&&
         post==answer.post_batch) return;
      auto[quality,finish]=simulateDecodePlan(
        jobs,pre,proc,post,answer.active_remotes,cycles);
      if(better(quality,finish)) {
        answer={pre,proc,post,answer.active_remotes,quality,finish};
        changed=true;
      }
    };
    for(int pass=0;pass<2;++pass) {
      changed=false;
      for(int size:sizes) improve(size,answer.proc_batch,answer.post_batch);
      for(int size:proc_sizes) improve(answer.batch_size,size,answer.post_batch);
      for(int size:sizes) improve(answer.batch_size,answer.proc_batch,size);
      if(!changed) break;
    }
  }

  if(active_count_>total) {
    auto scale=[&](int size) {
      return std::min(active_count_,
                      (size*active_count_+total-1)/total);
    };
    answer.batch_size=scale(answer.batch_size);
    answer.proc_batch=scale(answer.proc_batch);
    answer.post_batch=scale(answer.post_batch);
  }
  return answer;
}

bool prepareSchedulingData(){
if(prefill_pre_.empty()||prefill_proc_.empty()||
prefill_post_.empty()||decode_pre_.empty()||
decode_proc_.empty()||decode_post_.empty()){
return false;}std::sort(prefill_pre_.begin(),prefill_pre_.end());std::sort(prefill_proc_.begin(),prefill_proc_.end());std::sort(prefill_post_.begin(),prefill_post_.end());std::sort(decode_pre_.begin(),decode_pre_.end());std::sort(decode_proc_.begin(),decode_proc_.end());std::sort(decode_post_.begin(),decode_post_.end());best_decode_pre_batch_=bestBatchSizes(decode_pre_);best_decode_proc_batch_=bestBatchSizes(decode_proc_);best_decode_post_batch_=bestBatchSizes(decode_post_);const auto minimum=[](const TimingCurve&curve){
return std::min_element(curve.begin(),curve.end(),
[](const auto&lhs,const auto&rhs){
return lhs.second<rhs.second;})->second;};min_decode_pre_time_=minimum(decode_pre_);min_decode_post_time_=minimum(decode_post_);return true;}std::vector<int>oldestBatch(const ReadySet&ready,
       const std::vector<int>&choices)const{
if(!invariant(!ready.empty()&&ready.size()<choices.size())){
return{};}return oldestBatch(ready,choices[ready.size()]);}std::vector<int>oldestBatch(const ReadySet&ready,int count)const{
count=std::min(count,static_cast<int>(ready.size()));std::vector<int>request_ids;request_ids.reserve(usize(count));for(auto it=ready.begin();it!=ready.end()&&request_ids.size()<usize(count);++it){
request_ids.push_back(it->rid);}return request_ids;}int plannedTarget(const ReadySet&ready,int target,
const std::vector<int>&legacy)const{
return scoring_.throughput_weight>scoring_.waiting_weight?
std::max(target,legacy[ready.size()]):target;}int activeRequestCount()const{
return active_count_;}PipelinePlan currentPipelinePlan()const{
auto bucket=[&](int n){return usesJointDecodePlan()&&active_count_<=64?n:
n?1<<(std::bit_width((unsigned)n)-1):0;};int active=bucket(active_count_),decode=bucket(decode_count_);if(decode_count_==active_count_)decode+=kMaxRequests+1;if(cached_plan_active_!=active||cached_plan_decode_!=decode){
cached_plan_=planKnownDecodeWork();cached_plan_active_=active;cached_plan_decode_=decode;}return cached_plan_;}int prefillRemoteCount()const{
Real edge_work=0.0L;Real uplink_work=0.0L;Real remote_work=0.0L;Real downlink_work=0.0L;int remote_jobs=0;for(const std::optional<Request>&request:requests_){
if(!request||request->finished||
!isPrefillStage(request->stage)){
continue;}if(request->stage==RequestStage::READY_P_PRE){
edge_work+=
2.0L*system_.schedule_cost+
lookupTime(prefill_pre_,request->input_length)+
lookupTime(prefill_post_,request->input_length);}else if(request->stage<RequestStage::RUNNING_P_POST){
edge_work+=system_.schedule_cost+
     lookupTime(prefill_post_,request->input_length);}if(request->stage<=RequestStage::WAITING_P_UP){
uplink_work+=transferTime(request->input_length);}if(request->stage<=RequestStage::RUNNING_P_PROC){
remote_work+=
system_.schedule_cost+
lookupTime(prefill_proc_,request->input_length);++remote_jobs;}if(request->stage<=RequestStage::WAITING_P_DOWN){
downlink_work+=transferTime(request->input_length);}}const Real other_bottleneck=
std::max({edge_work,uplink_work,downlink_work});if(remote_jobs==0||remote_work<=other_bottleneck){
return 1;}if(other_bottleneck<=0.0L){
return std::min(system_.remote_count,remote_jobs);}const int required=static_cast<int>(
std::ceil(remote_work/other_bottleneck));return std::clamp(required,1,
   std::min(system_.remote_count,remote_jobs));}std::vector<int>decodePreBatch()const{
if(!usesJointDecodePlan()){
return oldestBatch(ready_d_pre_,plannedTarget(ready_d_pre_,
currentPipelinePlan().batch_size,best_decode_pre_batch_));}const PipelinePlan&plan=currentPipelinePlan();const usize target=std::min(
ready_d_pre_.size(),usize(plan.batch_size));std::vector<int>count(usize(system_.remote_count));for(const ReadyKey&key:ready_d_pre_){
++count[usize(requestAt(key.rid)->remote)];}std::vector<bool>selected(
usize(system_.remote_count),false);const int oldest_remote=requestAt(ready_d_pre_.begin()->rid)->remote;selected[usize(oldest_remote)]=true;std::vector<int>remotes;for(int remote=0;remote<system_.remote_count;++remote){
if(remote!=oldest_remote&&
count[usize(remote)]>0){
remotes.push_back(remote);}}std::sort(remotes.begin(),remotes.end(),[&](int lhs,int rhs){
return std::pair(-count[usize(lhs)],lhs)<
std::pair(-count[usize(rhs)],rhs);});int additional=std::min(
plan.active_remotes - 1,static_cast<int>(remotes.size()));for(auto it=remotes.begin();additional;--additional){
selected[usize(*it++)]=true;}std::vector<int>request_ids;for(const ReadyKey&key:ready_d_pre_){
const Request*request=requestAt(key.rid);if(selected[usize(request->remote)]){
request_ids.push_back(request->rid);if(request_ids.size()==target){
break;}}}return request_ids;}std::vector<int>decodeProcBatch(const ReadySet&ready)const{
if(!usesJointDecodePlan()){
return oldestBatch(ready,plannedTarget(ready,currentPipelinePlan().proc_batch,
best_decode_proc_batch_));}std::map<int,std::vector<int>>groups;for(const ReadyKey&key:ready){
const Request*request=requestAt(key.rid);if(!invariant(request!=nullptr&&request->decode_wave>=0)){
return{};}groups[request->decode_wave].push_back(request->rid);}const PipelinePlan&plan=currentPipelinePlan();const usize target=usize(
(plan.batch_size+plan.active_remotes - 1)/
plan.active_remotes);std::vector<int>request_ids;for(const auto&[wave,group]:groups){
static_cast<void>(wave);if(!request_ids.empty()&&
request_ids.size()+group.size()>target){
break;}request_ids.insert(request_ids.end(),group.begin(),group.end());if(request_ids.size()>=target){
break;}}return request_ids;}std::vector<int>completeDecodePostBatch()const{
std::vector<int>request_ids;if(!usesJointDecodePlan()||complete_decode_waves_.empty()){
return request_ids;}const int target=best_decode_post_batch_[complete_decode_members_];for(int id:complete_decode_waves_){
const auto wave=decode_waves_.find(id);if(!invariant(wave!=decode_waves_.end()))return{};if(!request_ids.empty()&&request_ids.size()+wave->second.request_ids.size()>
usize(target)){break;}request_ids.insert(request_ids.end(),wave->second.request_ids.begin(),
wave->second.request_ids.end());if(request_ids.size()>=usize(target))break;}return request_ids;}bool canFillDecodePost(
const std::vector<int>&joint_post_batch)const{
std::vector<bool>pre_remotes(
usize(system_.remote_count));for(int rid:decodePreBatch()){
pre_remotes[usize(requestAt(rid)->remote)]=true;}const std::vector<int>post_batch=usesJointDecodePlan()
?joint_post_batch
:oldestBatch(ready_d_post_,plannedTarget(ready_d_post_,
currentPipelinePlan().post_batch,best_decode_post_batch_));return std::all_of(post_batch.begin(),post_batch.end(),[&](int rid){
return pre_remotes[usize(requestAt(rid)->remote)];});}bool batchHasProducedToken(const std::vector<int>&request_ids)const{
return std::any_of(request_ids.begin(),request_ids.end(),
[&](int rid){
      return requestAt(rid)->has_produced_token;});}bool shouldAccumulateFirstDecodeWave()const{
if(ready_d_pre_.empty()||
ready_d_pre_.size()>=usize(
            currentPipelinePlan().batch_size)){
return false;}if(frontHasProducedToken(ready_d_pre_)){
PipelinePlan plan=currentPipelinePlan();auto jobs=knownDecodeJobs();int ready=ready_d_pre_.size();int depth=std::clamp(1024/(int)jobs.size(),4,16);auto start1=simulateDecodePlan(jobs,plan.batch_size,plan.proc_batch,
plan.post_batch,plan.active_remotes,1,ready).first;auto wait1=simulateDecodePlan(jobs,plan.batch_size,plan.proc_batch,
plan.post_batch,plan.active_remotes,1,plan.batch_size).first;if(wait1<=start1)return false;auto startN=simulateDecodePlan(jobs,plan.batch_size,plan.proc_batch,
plan.post_batch,plan.active_remotes,depth,ready).first;auto waitN=simulateDecodePlan(jobs,plan.batch_size,plan.proc_batch,
plan.post_batch,plan.active_remotes,depth,plan.batch_size).first;if(waitN<=startN)return false;}Real next_decode_ready=
kInfinity;for(const std::optional<Request>&request:requests_){
if(request&&!request->finished&&
request->stage!=RequestStage::READY_D_PRE){
RequestStage stage=request->stage;bool guaranteed=stage==RequestStage::RUNNING_P_PRE||
stage==RequestStage::WAITING_P_UP||stage==RequestStage::RUNNING_P_PROC||
stage==RequestStage::WAITING_P_DOWN||stage==RequestStage::RUNNING_P_POST||
stage==RequestStage::RUNNING_D_PRE||stage==RequestStage::WAITING_D_UP||
stage==RequestStage::RUNNING_D_PROC||stage==RequestStage::WAITING_D_DOWN||
stage==RequestStage::RUNNING_D_POST;if(!guaranteed)continue;Real release=frontierRelease(*request);if(release>0)next_decode_ready=std::min<Real>(
next_decode_ready,current_time_+release);}}if(!std::isfinite(next_decode_ready)){
return false;}const std::vector<int>batch=decodePreBatch();const Real task_done=
current_time_+system_.schedule_cost+
lookupTime(decode_pre_,static_cast<int>(batch.size()));const Real uplink_done=
std::max<Real>(task_done,uplink_free_time_)+
transferTime(static_cast<int>(batch.size()));return next_decode_ready<=uplink_done;}void createDecodeWave(const std::vector<int>&request_ids){
const int wave_id=next_decode_wave_++;DecodeWave wave{request_ids,0};for(int rid:request_ids){
Request*request=requestAt(rid);static_cast<void>(invariant(request!=nullptr&&
          request->decode_wave==-1));request->decode_wave=wave_id;}decode_waves_.emplace(wave_id,std::move(wave));}void markDecodePostReady(int rid){
Request*request=requestAt(rid);if(request->decode_wave<0){
return;}auto wave=decode_waves_.find(request->decode_wave);if(!invariant(wave!=decode_waves_.end()&&
  wave->second.ready_post<
    static_cast<int>(wave->second.request_ids.size()))){
return;}++wave->second.ready_post;if(wave->second.ready_post==
static_cast<int>(wave->second.request_ids.size())){
complete_decode_waves_.insert(wave->first);complete_decode_members_+=wave->second.ready_post;}}void detachDecodePostBatch(const std::vector<int>&request_ids){
std::map<int,std::vector<int>>selected_by_wave;for(int rid:request_ids){
Request*request=requestAt(rid);if(request->decode_wave>=0){
selected_by_wave[request->decode_wave].push_back(rid);}}for(const auto&[wave_id,selected]:selected_by_wave){
auto wave=decode_waves_.find(wave_id);if(!invariant(wave!=decode_waves_.end()&&
    wave->second.ready_post>=
      static_cast<int>(selected.size()))){
return;}if(complete_decode_waves_.erase(wave_id)!=0){
complete_decode_members_-=
static_cast<int>(wave->second.request_ids.size());}std::set<int>removed(selected.begin(),selected.end());std::vector<int>remaining;remaining.reserve(wave->second.request_ids.size()-
     selected.size());for(int rid:wave->second.request_ids){
if(removed.contains(rid)){
requestAt(rid)->decode_wave=-1;}else{
remaining.push_back(rid);}}wave->second.ready_post-=static_cast<int>(selected.size());wave->second.request_ids=std::move(remaining);if(wave->second.request_ids.empty()){
decode_waves_.erase(wave);}else if(wave->second.ready_post==
  static_cast<int>(wave->second.request_ids.size())){
complete_decode_waves_.insert(wave_id);complete_decode_members_+=wave->second.ready_post;}}}int placementRemoteLimit()const{
return usesJointDecodePlan()
?std::max(currentPipelinePlan().active_remotes,prefillRemoteCount())
:system_.remote_count;}int leastLoadedRemote(int remote_limit)const{
const int requests_per_remote=
(activeRequestCount()+remote_limit - 1)/remote_limit;const int decode_batch=best_decode_proc_batch_[
usize(requests_per_remote)];const Real decode_quantum=
(system_.schedule_cost+lookupTime(decode_proc_,decode_batch))/
decode_batch;const auto load=[&](int remote){return prefill_load_[remote]+
active_by_remote_[remote]*decode_quantum;};const int first=next_remote_%remote_limit;int best=first;for(int offset=1;offset<remote_limit;++offset){
const int remote=(first+offset)%remote_limit;if(load(remote)+1e-12L*std::max(1.L,std::abs(load(best)))<load(best)){
best=remote;}}return best;}static bool isPrefillStage(RequestStage stage){
return stage>=RequestStage::READY_P_PRE&&
stage<=RequestStage::RUNNING_P_POST;}Real transferTime(int token_count)const{
return system_.latency_in_ms+
8.0L*token_count*system_.bytes_per_token/
(Real(system_.bandwidth_gbps)*1.0e6L);}Real estimatedPrefillTime(const Request&request)const{
return 3.0L*system_.schedule_cost+
lookupTime(prefill_pre_,request.input_length)+
lookupTime(prefill_proc_,request.input_length)+
lookupTime(prefill_post_,request.input_length)+
2.0L*transferTime(request.input_length);}const Request*shortestPrefill(const ReadySet&)const{
return requestAt(ready_p_pre_short_.begin()->second);}double normalizedLateness(const Request&request)const{
const bool prefill=isPrefillStage(request.stage);if(!prefill&&!request.has_produced_token){
return -std::numeric_limits<double>::infinity();}const double target=prefill?scoring_.slo1:scoring_.slo2;const double reference=
prefill?request.arrival_time:request.decode_ready_time;return(current_time_ - reference - target)/target;}bool costlyPrefill(const Request&request)const{
if(scoring_.waiting_weight==0.0){
return false;}const double budget=scoring_.slo2/scoring_.waiting_weight;const Real transfer=transferTime(request.input_length);const double pre=system_.schedule_cost+
    lookupTime(prefill_pre_,request.input_length);const double proc=system_.schedule_cost+
    lookupTime(prefill_proc_,request.input_length);const double post=system_.schedule_cost+
    lookupTime(prefill_post_,request.input_length);return transfer>budget||pre>budget||
proc>budget||post>budget;}Real decodeProcReadyTime(const Request&request)const{
const Real edge_ready=edge_task_
?edge_task_finish_time_
:current_time_;const Real pre_duration=system_.schedule_cost+
min_decode_pre_time_;const Real post_duration=system_.schedule_cost+
min_decode_post_time_;const auto after_pre=[&](Real start){
const Real done=start+pre_duration;return std::max<Real>(done,uplink_free_time_)+
transferTime(1);};switch(request.stage){
case RequestStage::READY_D_PRE:
return after_pre(edge_ready);case RequestStage::RUNNING_D_PRE:{
int members=1;if(edge_task_){
members=static_cast<int>(std::count_if(
  edge_task_->request_ids.begin(),edge_task_->request_ids.end(),
[&](int rid){return requestAt(rid)->remote==request.remote;}));}return std::max<Real>(edge_task_finish_time_,
               uplink_free_time_)+
transferTime(members);}case RequestStage::WAITING_D_UP:
return request.transfer_ready_time;case RequestStage::READY_D_PROC:
case RequestStage::RUNNING_D_PROC:
return current_time_;case RequestStage::WAITING_D_DOWN:
return after_pre(
std::max<Real>(edge_ready,request.transfer_ready_time)+
post_duration);case RequestStage::READY_D_POST:
return after_pre(edge_ready+post_duration);case RequestStage::RUNNING_D_POST:
return after_pre(edge_task_finish_time_);default:
return kInfinity;}}int prefillLayerEnd(const Request&request,int remote)const{
if(scoring_.waiting_weight==0.0||system_.num_layers==1||
normalizedLateness(request)>=0.0){
return system_.num_layers;}const int remaining=system_.num_layers - request.next_prefill_layer;const Real full_duration=
lookupTime(prefill_proc_,request.input_length)*remaining/
system_.num_layers;Real next_decode_ready=
kInfinity;bool active_decode=false;for(const std::optional<Request>&other:requests_){
if(!other||other->finished||other->remote!=remote||
other->stage<RequestStage::READY_D_PRE||
other->stage>RequestStage::RUNNING_D_POST){
continue;}active_decode|=other->has_produced_token;next_decode_ready=std::min(
next_decode_ready,decodeProcReadyTime(*other));}if(!std::isfinite(next_decode_ready)||
next_decode_ready>=
current_time_+system_.schedule_cost+full_duration){
return system_.num_layers;}if(!active_decode){
return system_.num_layers;}const Real piece_budget=std::max<Real>(
scoring_.slo2/scoring_.waiting_weight,
next_decode_ready - current_time_);if(system_.schedule_cost+full_duration<=piece_budget){
return system_.num_layers;}const Real layer_duration=
lookupTime(prefill_proc_,request.input_length)/system_.num_layers;const Real compute_budget=
std::max(0.0L,piece_budget - system_.schedule_cost);const Real affordable_layers=
std::floor(compute_budget/layer_duration);const int layers=affordable_layers>=remaining
?remaining
:std::max(1,static_cast<int>(affordable_layers));return request.next_prefill_layer+layers;}bool prefillCanWait(const Request&candidate)const{
if(scoring_.waiting_weight==0.0||
normalizedLateness(candidate)>=0.0||
!costlyPrefill(candidate)||
current_time_ - candidate.arrival_time+
estimatedPrefillTime(candidate)>=scoring_.slo1){
return false;}return true;}bool shouldDeferPrefill(const Request&candidate)const{
if(!prefillCanWait(candidate)){
return false;}bool active_decode=false;bool guaranteed_event=edge_task_.has_value()||
      std::any_of(remote_tasks_.begin(),remote_tasks_.end(),
[](const auto&task){return task.has_value();});for(const std::optional<Request>&request:requests_){
if(!request||request->finished){
continue;}if(request->stage>=RequestStage::READY_D_PRE&&
request->stage<=RequestStage::RUNNING_D_POST){
active_decode|=request->has_produced_token;}guaranteed_event|=
request->stage==RequestStage::WAITING_P_UP||
request->stage==RequestStage::WAITING_P_DOWN||
request->stage==RequestStage::WAITING_D_UP||
request->stage==RequestStage::WAITING_D_DOWN;}return active_decode&&guaranteed_event;}const Request*mostOverdue(
std::initializer_list<const ReadySet*>ready_sets)const{
const Request*best=nullptr;double best_lateness=-1.0;for(const ReadySet*ready:ready_sets){
if(ready->empty()){
continue;}const Request*request=requestAt(ready->begin()->rid);if(!invariant(request!=nullptr)){
return nullptr;}const double lateness=normalizedLateness(*request);const double threshold=
request->stage==RequestStage::READY_P_PRE&&
  scoring_.waiting_weight!=0.0
?std::max(1.0,scoring_.distance_base)
:0.0;if(lateness<threshold){
continue;}if(best==nullptr||lateness>best_lateness||
(lateness==best_lateness&&
 std::pair(request->arrival_time,request->rid)<
 std::pair(best->arrival_time,best->rid))){
best=request;best_lateness=lateness;}}return best;}Real batchDrainFinish(
usize ready_count,const std::vector<double>&transfer_times,
int target,const TimingCurve&curve,
bool wait_for_transfer)const{
Real time=current_time_;usize next_transfer=0;auto collectTransfers=[&]{
while(next_transfer<transfer_times.size()&&
 transfer_times[next_transfer]<=time){
++ready_count;++next_transfer;}};if(wait_for_transfer){
time=std::max<Real>(time,transfer_times.front());collectTransfers();}while(ready_count!=0||next_transfer<transfer_times.size()){
if(ready_count==0){
time=std::max<Real>(time,
             transfer_times[next_transfer]);collectTransfers();}const int batch=std::min(target,static_cast<int>(ready_count));ready_count-=usize(batch);time+=system_.schedule_cost+lookupTime(curve,batch);collectTransfers();}return time;}bool shouldDeferBatch(const ReadySet&ready,
   int target,
   const TimingCurve&curve,
   RequestStage pending_stage,int remote)const{
if(ready.empty()||(!favorsThroughput()&&
mostOverdue({&ready})!=nullptr)){
return false;}std::vector<double>transfer_times;for(const std::optional<Request>&request:requests_){
if(request&&!request->finished&&
request->stage==pending_stage&&
(remote<0||request->remote==remote)){
transfer_times.push_back(request->transfer_ready_time);}}if(pending_stage==RequestStage::WAITING_D_UP&&edge_task_&&
edge_task_->work==WorkKind::DECODE&&edge_task_->step==TaskStep::PRE){
int count=0;for(int rid:edge_task_->request_ids)count+=requestAt(rid)->remote==remote;if(count){
double ready=std::max(edge_task_finish_time_,uplink_free_time_)+
transferTime(count);for(int i=0;i<count;++i)transfer_times.push_back(ready);}}if(pending_stage==RequestStage::WAITING_D_DOWN){
std::vector<std::pair<double,int>>running;for(int r=0;r<system_.remote_count;++r){if(remote_tasks_[r]&&
remote_tasks_[r]->work==WorkKind::DECODE&&
remote_tasks_[r]->step==TaskStep::PROC)
running.emplace_back(remote_task_finish_time_[r],
remote_tasks_[r]->request_ids.size());}std::sort(running.begin(),running.end());double link=downlink_free_time_;for(auto[finish,count]:running){
link=std::max(link,finish)+transferTime(count);for(int i=0;i<count;++i)transfer_times.push_back(link);}}if(transfer_times.empty()){
return false;}if(std::min<usize>(target,ready.size()+transfer_times.size())<=
std::min<usize>(target,ready.size())){
return false;}std::sort(transfer_times.begin(),transfer_times.end());const Real start_finish=batchDrainFinish(
ready.size(),transfer_times,target,curve,false);const Real wait_finish=batchDrainFinish(
ready.size(),transfer_times,target,curve,true);const Real scale=
std::max({1.0L,std::abs(start_finish),std::abs(wait_finish)});return wait_finish+1.0e-12L*scale<start_finish;}static bool atEnd(std::istringstream&input){
input>>std::ws;return input.eof();}template<class T>
static bool parseSingleValue(const std::string&line,T&value){
std::istringstream input(line);return static_cast<bool>(input>>value)&&atEnd(input);}static bool parseServer(const std::string&token,int&server){
if(token=="E"){
server=-1;return true;}if(token.size()<2||token.front()!='C'){
return false;}std::istringstream input(token.substr(1));return static_cast<bool>(input>>server)&&server>=0&&atEnd(input);}static bool parseCompletedTask(std::istringstream&input,TaskSpec&task,
        double&duration){
std::string work;std::string step;if(!(input>>work>>step)){
return false;}if(work=="P"){
task.work=WorkKind::PREFILL;}else if(work=="D"){
task.work=WorkKind::DECODE;}else{
return false;}if(step=="PRE"){
task.step=TaskStep::PRE;}else if(step=="PROC"){
task.step=TaskStep::PROC;}else if(step=="POST"){
task.step=TaskStep::POST;}else{
return false;}if(task.work==WorkKind::PREFILL){
int rid=-1;if(task.step==TaskStep::PROC){
if(!(input>>task.layer_start>>task.layer_end>>task.remote>>
 rid>>duration)){
return false;}}else if(!(input>>task.remote>>rid>>duration)){
return false;}task.request_ids.push_back(rid);}else{
int count=0;if(!(input>>task.remote>>count)||count<1||count>2000){
return false;}task.request_ids.resize(usize(count));for(int&rid:task.request_ids){
if(!(input>>rid)){
return false;}}if(!(input>>duration)){
return false;}}return std::isfinite(duration)&&atEnd(input);}std::optional<Event>parseEvent(const std::string&line)const{
std::istringstream input(line);std::string kind;if(!(input>>kind)){
return std::nullopt;}Event event;if(kind=="ARR"){
event.kind=EventKind::ARRIVAL;if(!(input>>event.rid>>event.input_length)||!atEnd(input)){
return std::nullopt;}return event;}if(kind=="FIN"){
event.kind=EventKind::FINISH;if(!(input>>event.rid)||!atEnd(input)){
return std::nullopt;}return event;}if(kind=="TDN"){
event.kind=EventKind::TASK_DONE;std::string server;if(!(input>>server)||!parseServer(server,event.server)||
!parseCompletedTask(input,event.task,event.duration)){
return std::nullopt;}return event;}if(kind=="XDN"){
event.kind=EventKind::TRANSFER_DONE;std::string direction;std::string transfer_kind;int count=0;if(!(input>>direction>>event.remote>>event.size>>
 transfer_kind>>count)||
count<1||count>2000){
return std::nullopt;}if(direction=="UP"){
event.direction=Direction::UP;}else if(direction=="DOWN"){
event.direction=Direction::DOWN;}else{
return std::nullopt;}if(transfer_kind=="PRE"){
event.transfer_kind=TransferKind::PREFILL;}else if(transfer_kind=="DEC"){
event.transfer_kind=TransferKind::DECODE;}else{
return std::nullopt;}event.request_ids.resize(usize(count));for(int&rid:event.request_ids){
if(!(input>>rid)){
return std::nullopt;}}if(!atEnd(input)){
return std::nullopt;}return event;}return std::nullopt;}Request*requestAt(int rid){
if(rid<0||usize(rid)>=requests_.size()||
!requests_[usize(rid)]){
return nullptr;}return&*requests_[usize(rid)];}const Request*requestAt(int rid)const{
if(rid<0||usize(rid)>=requests_.size()||
!requests_[usize(rid)]){
return nullptr;}return&*requests_[usize(rid)];}static ReadyKey readyKey(const Request&request){
const bool prefill=isPrefillStage(request.stage);const double priority_time=
prefill?request.arrival_time:request.decode_ready_time;return{!prefill&&!request.has_produced_token,priority_time,
request.rid};}bool frontHasProducedToken(const ReadySet&ready)const{
if(ready.empty()){
return false;}const Request*request=requestAt(ready.begin()->rid);return invariant(request!=nullptr)&&request->has_produced_token;}ReadySet*readySetFor(const Request&request){
switch(request.stage){
case RequestStage::READY_P_PRE:
return&ready_p_pre_;case RequestStage::READY_P_PROC:
return&ready_p_proc_[usize(request.remote)];case RequestStage::READY_P_POST:
return&ready_p_post_;case RequestStage::READY_D_PRE:
return&ready_d_pre_;case RequestStage::READY_D_PROC:
return&ready_d_proc_[usize(request.remote)];case RequestStage::READY_D_POST:
return&ready_d_post_;default:
return nullptr;}}void addReady(const Request&request){
ReadySet*ready=readySetFor(request);if(ready!=nullptr){
static_cast<void>(invariant(ready->insert(readyKey(request)).second));}if(request.stage==RequestStage::READY_P_PRE)
ready_p_pre_short_.emplace(estimatedPrefillTime(request),request.rid);}void removeReady(const Request&request){
ReadySet*ready=readySetFor(request);if(ready!=nullptr){
static_cast<void>(invariant(ready->erase(readyKey(request))==1));}if(request.stage==RequestStage::READY_P_PRE)
ready_p_pre_short_.erase({estimatedPrefillTime(request),request.rid});}void setStage(Request&request,RequestStage stage){
removeReady(request);decode_count_+=(stage>=RequestStage::READY_D_PRE&&
stage<=RequestStage::RUNNING_D_POST)-
(request.stage>=RequestStage::READY_D_PRE&&
request.stage<=RequestStage::RUNNING_D_POST);request.stage=stage;addReady(request);}static RequestStage readyStageFor(const TaskSpec&task){
if(task.work==WorkKind::PREFILL){
if(task.step==TaskStep::PRE){
return RequestStage::READY_P_PRE;}if(task.step==TaskStep::PROC){
return RequestStage::READY_P_PROC;}return RequestStage::READY_P_POST;}if(task.step==TaskStep::PRE){
return RequestStage::READY_D_PRE;}if(task.step==TaskStep::PROC){
return RequestStage::READY_D_PROC;}return RequestStage::READY_D_POST;}static RequestStage runningStageFor(const TaskSpec&task){
return static_cast<RequestStage>(
static_cast<int>(readyStageFor(task))+1);}RequestStage completedStageFor(const TaskSpec&task)const{
if(task.work==WorkKind::PREFILL){
if(task.step==TaskStep::PRE){
return RequestStage::WAITING_P_UP;}if(task.step==TaskStep::PROC){
return task.layer_end==system_.num_layers
?RequestStage::WAITING_P_DOWN
:RequestStage::READY_P_PROC;}return RequestStage::READY_D_PRE;}if(task.step==TaskStep::PRE){
return RequestStage::WAITING_D_UP;}if(task.step==TaskStep::PROC){
return RequestStage::WAITING_D_DOWN;}return RequestStage::READY_D_PRE;}int expectedServer(const TaskSpec&task)const{
return task.step==TaskStep::PROC?task.remote:-1;}bool validateTaskShape(const TaskSpec&task)const{
if(!invariant(!task.request_ids.empty())){
return false;}std::set<int>unique_ids(task.request_ids.begin(),task.request_ids.end());if(!invariant(unique_ids.size()==task.request_ids.size())){
return false;}const bool valid_remote=
task.remote>=0&&task.remote<system_.remote_count;if(task.work==WorkKind::PREFILL){
if(!invariant(task.request_ids.size()==1&&valid_remote)){
return false;}if(task.step==TaskStep::PROC){
return invariant(task.layer_start>=0&&
       task.layer_start<task.layer_end&&
       task.layer_end<=system_.num_layers);}return invariant(task.layer_start==-1&&task.layer_end==-1);}if(!invariant(task.layer_start==-1&&task.layer_end==-1)){
return false;}if(task.step==TaskStep::PROC){
return invariant(valid_remote);}return invariant(task.remote==-1);}bool validateRequests(const TaskSpec&task,RequestStage expected)const{
for(int rid:task.request_ids){
const Request*request=requestAt(rid);if(!invariant(request!=nullptr&&!request->finished&&
    request->stage==expected)){
return false;}if((task.work==WorkKind::PREFILL||
 task.step==TaskStep::PROC)&&
!invariant(request->remote==task.remote)){
return false;}if(task.work==WorkKind::PREFILL&&
task.step==TaskStep::PROC&&
!invariant(request->next_prefill_layer==task.layer_start)){
return false;}}return true;}bool startAssignment(const Assignment&assignment){
if(!validateTaskShape(assignment.task)||
!invariant(assignment.server==expectedServer(assignment.task))||
!validateRequests(assignment.task,readyStageFor(assignment.task))){
return false;}if(assignment.server==-1){
if(!invariant(!edge_task_)){
return false;}}else{
const usize remote=usize(assignment.server);if(!invariant(!remote_tasks_[remote])){
return false;}}if(assignment.task.work==WorkKind::DECODE&&
assignment.task.step==TaskStep::PRE){
createDecodeWave(assignment.task.request_ids);}else if(assignment.task.work==WorkKind::DECODE&&
assignment.task.step==TaskStep::POST){
detachDecodePostBatch(assignment.task.request_ids);}if(assignment.server==-1){
const int size=assignment.task.work==WorkKind::PREFILL
?requestAt(assignment.task.request_ids.front())->input_length
:static_cast<int>(assignment.task.request_ids.size());const TimingCurve&curve=assignment.task.work==WorkKind::PREFILL
?(assignment.task.step==TaskStep::PRE
?prefill_pre_:prefill_post_)
:(assignment.task.step==TaskStep::PRE
?decode_pre_:decode_post_);edge_task_finish_time_=current_time_+system_.schedule_cost+
lookupTime(curve,size);edge_task_=assignment.task;}else{
const usize remote=usize(assignment.server);remote_tasks_[remote]=assignment.task;int size=assignment.task.work==WorkKind::PREFILL?
requestAt(assignment.task.request_ids.front())->input_length:
static_cast<int>(assignment.task.request_ids.size());double duration=lookupTime(assignment.task.work==WorkKind::PREFILL?
prefill_proc_:decode_proc_,size);if(assignment.task.work==WorkKind::PREFILL){duration*=
(assignment.task.layer_end-assignment.task.layer_start)/
static_cast<double>(system_.num_layers);}remote_task_finish_time_[remote]=current_time_+system_.schedule_cost+duration;}if(assignment.task.work==WorkKind::PREFILL&&
assignment.task.step==TaskStep::PRE){
const int remote=assignment.task.remote;++active_by_remote_[remote];prefill_load_[remote]+=system_.schedule_cost+
lookupTime(prefill_proc_,requestAt(
assignment.task.request_ids[0])->input_length);}for(int rid:assignment.task.request_ids){
setStage(*requestAt(rid),runningStageFor(assignment.task));}return true;}bool applyEvent(const Event&event,double timestamp){
if(event.kind==EventKind::ARRIVAL){
return applyArrival(event,timestamp);}if(event.kind==EventKind::TASK_DONE){
return applyTaskDone(event,timestamp);}if(event.kind==EventKind::TRANSFER_DONE){
return applyTransferDone(event);}return false;}bool applyArrival(const Event&event,double timestamp){
if(!invariant(event.rid>=0&&event.input_length>0)){
return false;}const usize rid=usize(event.rid);if(requests_.size()<=rid){
requests_.resize(rid+1);}if(!invariant(!requests_[rid])){
return false;}requests_[rid]=Request{event.rid,event.input_length,-1,
       RequestStage::READY_P_PRE,timestamp,timestamp,
       false};++active_count_;addReady(*requests_[rid]);return true;}void recordTransfer(Direction direction,int token_count,
  const std::vector<int>&request_ids,
  double timestamp){
double&link_free=direction==Direction::UP?uplink_free_time_
:downlink_free_time_;link_free=static_cast<double>(
std::max<Real>(timestamp,link_free)+
transferTime(token_count));for(int rid:request_ids){
requestAt(rid)->transfer_ready_time=link_free;}}void recordTaskTransfers(const TaskSpec&task,double timestamp){
if(task.work==WorkKind::PREFILL){
const Request*request=requestAt(task.request_ids.front());if(task.step==TaskStep::PRE){
recordTransfer(Direction::UP,request->input_length,
      task.request_ids,timestamp);}else if(task.step==TaskStep::PROC&&
  task.layer_end==system_.num_layers){
recordTransfer(Direction::DOWN,request->input_length,
      task.request_ids,timestamp);}return;}if(task.step==TaskStep::PRE){
std::map<int,std::vector<int>>by_remote;for(int rid:task.request_ids){
by_remote[requestAt(rid)->remote].push_back(rid);}for(const auto&[remote,request_ids]:by_remote){
static_cast<void>(remote);recordTransfer(Direction::UP,
      static_cast<int>(request_ids.size()),
      request_ids,timestamp);}}else if(task.step==TaskStep::PROC){
recordTransfer(Direction::DOWN,
    static_cast<int>(task.request_ids.size()),
    task.request_ids,timestamp);}}bool applyTaskDone(const Event&event,double timestamp){
if(!validateTaskShape(event.task)||
!invariant(event.server==expectedServer(event.task))||
!validateRequests(event.task,runningStageFor(event.task))){
return false;}if(event.server==-1){
if(!invariant(edge_task_&&*edge_task_==event.task)){
return false;}edge_task_.reset();}else{
const usize remote=usize(event.server);if(!invariant(remote<remote_tasks_.size()&&remote_tasks_[remote]&&
*remote_tasks_[remote]==event.task)){
return false;}remote_tasks_[remote].reset();}recordTaskTransfers(event.task,timestamp);const RequestStage next=completedStageFor(event.task);for(int rid:event.task.request_ids){
Request*request=requestAt(rid);if(event.task.work==WorkKind::PREFILL&&
event.task.step==TaskStep::PROC){
prefill_load_[event.task.remote]-=
lookupTime(prefill_proc_,request->input_length)*
(event.task.layer_end-event.task.layer_start)/system_.num_layers+
(event.task.layer_end==system_.num_layers?system_.schedule_cost:0);request->next_prefill_layer=event.task.layer_end;if(event.task.layer_end<system_.num_layers){
yield_to_decode_[usize(event.task.remote)]=
  true;}}if(event.task.step==TaskStep::POST){
request->decode_ready_time=timestamp;request->has_produced_token|=
event.task.work==WorkKind::DECODE;}setStage(*request,next);}return true;}bool applyTransferDone(const Event&event){
if(!invariant(event.remote>=0&&
  event.remote<system_.remote_count&&event.size>=0)){
return false;}std::set<int>unique_ids(event.request_ids.begin(),
       event.request_ids.end());if(!invariant(!event.request_ids.empty()&&
  unique_ids.size()==event.request_ids.size())){
return false;}RequestStage expected;RequestStage next;std::int64_t expected_size=0;if(event.transfer_kind==TransferKind::PREFILL){
if(!invariant(event.request_ids.size()==1)){
return false;}const Request*request=requestAt(event.request_ids.front());if(!invariant(request!=nullptr)){
return false;}expected_size=static_cast<std::int64_t>(request->input_length)*
    system_.bytes_per_token;if(event.direction==Direction::UP){
expected=RequestStage::WAITING_P_UP;next=RequestStage::READY_P_PROC;}else{
expected=RequestStage::WAITING_P_DOWN;next=RequestStage::READY_P_POST;}}else{
expected_size=static_cast<std::int64_t>(event.request_ids.size())*
    system_.bytes_per_token;if(event.direction==Direction::UP){
expected=RequestStage::WAITING_D_UP;next=RequestStage::READY_D_PROC;}else{
expected=RequestStage::WAITING_D_DOWN;next=RequestStage::READY_D_POST;}}if(!invariant(event.size==expected_size)){
return false;}for(int rid:event.request_ids){
const Request*request=requestAt(rid);if(!invariant(request!=nullptr&&!request->finished&&
    request->remote==event.remote&&
    request->stage==expected)){
return false;}}for(int rid:event.request_ids){
setStage(*requestAt(rid),next);requestAt(rid)->transfer_ready_time=0.0;if(next==RequestStage::READY_D_POST){
markDecodePostReady(rid);}}return true;}bool applyFinish(int rid){
Request*request=requestAt(rid);if(!invariant(request!=nullptr&&!request->finished&&
  request->stage==RequestStage::READY_D_PRE)){
return false;}setStage(*request,RequestStage::FINISHED);request->finished=true;--active_by_remote_[request->remote];--active_count_;return true;}void writeTaskSpec(const TaskSpec&task){
output_<<(task.work==WorkKind::PREFILL?'P':'D')<<' ';if(task.step==TaskStep::PRE){
output_<<"PRE";}else if(task.step==TaskStep::PROC){
output_<<"PROC";}else{
output_<<"POST";}if(task.work==WorkKind::PREFILL&&task.step==TaskStep::PROC){
output_<<' '<<task.layer_start<<' '<<task.layer_end;}output_<<' '<<task.remote;if(task.work==WorkKind::DECODE){
output_<<' '<<task.request_ids.size();}for(int rid:task.request_ids){
output_<<' '<<rid;}}std::istream&input_;std::ostream&output_;SystemConfig system_;ScoringConfig scoring_;TimingCurve prefill_pre_;TimingCurve prefill_proc_;TimingCurve prefill_post_;TimingCurve decode_pre_;TimingCurve decode_proc_;TimingCurve decode_post_;std::vector<int>best_decode_pre_batch_;std::vector<int>best_decode_proc_batch_;std::vector<int>best_decode_post_batch_;double min_decode_pre_time_=0.0;double min_decode_post_time_=0.0;std::vector<std::optional<Request>>requests_;int active_count_=0,decode_count_=0;double current_time_=0.0;double uplink_free_time_=0.0;double downlink_free_time_=0.0;double edge_task_finish_time_=0.0;std::vector<bool>yield_to_decode_;bool deferred_d_pre_=false;std::optional<TaskSpec>edge_task_;std::vector<std::optional<TaskSpec>>remote_tasks_;std::vector<double>remote_task_finish_time_;std::vector<int>active_by_remote_;std::vector<Real>prefill_load_;int next_remote_=0;int next_decode_wave_=0;std::map<int,DecodeWave>decode_waves_;std::set<int>complete_decode_waves_;int complete_decode_members_=0;ReadySet ready_p_pre_;std::set<std::pair<Real,int>>ready_p_pre_short_;ReadySet ready_p_post_;ReadySet ready_d_pre_;ReadySet ready_d_post_;std::vector<ReadySet>ready_p_proc_;std::vector<ReadySet>ready_d_proc_;mutable PipelinePlan cached_plan_;mutable int cached_plan_active_=-1,cached_plan_decode_=-1;};int main(){
std::ios::sync_with_stdio(false);std::cin.tie(nullptr);Scheduler scheduler(std::cin,std::cout);if(!scheduler.readStartup()){
return 0;}while(scheduler.readAndProcessFrame()){
}return 0;}
