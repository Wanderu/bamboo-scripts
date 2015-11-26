--[[

fail <ns> , <jobid> <datetime> <requeue_seconds>

Keys:
    ns: Namespace under which queue data exists.

Args:
    jobid: Job identifier.
    datetime: Current datetime (Unix seconds since epoch)
    requeue_seconds: Seconds until requeue.

Returns: 1 if failed correctly.
Errors: UNKNOWN_JOB_ID

--]]

-- ########### PRE
local fname = "fail"
local sep = ":"

local log_warn = function (message)
    redis.log(redis.LOG_WARNING, "<" .. fname .. ">" .. " " .. message)
end

local log_verbose = function (message)
    redis.log(redis.LOG_VERBOSE, "<" .. fname .. ">" .. " " .. message)
end

local is_error = function(result)
    return type(result) == 'table' and result.err
end
-- ########### END PRE

local ns = KEYS[1]
local jobid = ARGV[1]
local dtutcnow = tonumber(ARGV[2])
if dtuctnow == nil then
    return redis.error_reply("INVALID_PARAMETER: datetime")
end
local requeue_seconds = tonumber(ARGV[3])
if requeue_seconds == nil then
    return redis.error_reply("INVALID_PARAMETER: requeue_seconds")
end

local dtreschedule = dtutcnow + requeue_seconds

local kworking   = ns .. sep .. "WORKING"   -- Jobs that have been consumed
local kworkers   = ns .. sep .. "WORKERS"   -- Worker IDs
local kfailed    = ns .. sep .. "FAILED"    -- Failed Queue
local kscheduled = ns .. sep .. "SCHEDULED" -- Scheduled Queue
local kmaxfailed = ns .. sep .. "MAXFAILED" -- Max number of failures allowed

local kjob = ns .. sep .. "JOBS" .. sep .. jobid
local result

result = redis.pcall("ZSCORE", kworking, jobid)
if result == nil or is_error(result) then
    log_warn("Provided job not found. Job ID: " .. jobid .. "Queue: " .. kworking)
    return redis.error_reply("UNKNOWN_JOB_ID: " ..
                             "Job not found in queue." .. kworking)
end

-- ######################
-- Remove job from WORKING queue
-- ######################
redis.call("ZREM", kworking, jobid)

-- ######################
-- Increment failure count
-- Set failure date
-- ######################
local failures = tonumber(redis.pcall("HGET", kjob, "failures"))
if failures == nil then failures = 0 end
redis.call("HMSET", kjob,
           "failures", tostring(failures+1),
           "datefailed", tostring(dtutcnow))

-- ######################
-- Either add to SCHEDULED or FAILED queues depending on MAX_FAILED
-- ######################
local maxfailed = tonumber(redis.pcall("GET", kmaxfailed))
if maxfailed == nil or failures >= maxfailed then
    -- ######################
    -- Move to FAILED queue, remove Job data
    -- ######################
    result = redis.pcall("ZADD", kfailed, dtutcnow, jobid);
    result = redis.pcall("DEL", kjob);
else
    -- ######################
    -- Move to SCHEDULED queue, keep Job data
    -- ######################
    result = redis.pcall("ZADD", kscheduled, dtreschedule, jobid);
end

-- ######################
-- Remove job from worker's set
-- ######################
local client_name = redis.call("HGET", kjob, "owner")
if client_name == "" or client_name == false then
    log_warn("No worker registered/found for job.")
else
    local kworker = kworkers .. sep .. client_name
    result = redis.pcall("SREM", kworker, jobid)
    -- If there are no more outstanding jobs, remove the worker from the set of
    -- workers.
    local njobs = redis.call("SCARD", kworker)
    if njobs <= 0 then
        redis.call("SREM", ns, client_name)
    end
end

return 1
