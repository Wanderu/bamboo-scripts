--[[

fail <ns> , <jobid> <datetime> <wait_base> <wait_exp>

Keys:
    ns: Namespace under which queue data exists.

Args:
    jobid: Job identifier.
    datetime: Current datetime as Unix UTC seconds since epoch. Required to be
            passed in due to limitations of Lua engine inside Redis.
    wait_base, wait_exp: Requeue priority (SCHEDULED queue) will be equal to
    datetime + (wait_base * failures^wait_exp)

Returns: 1 if failed correctly.
Errors: UNKNOWN_JOB_ID

--]]

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

local ns = KEYS[1]
local jobid = ARGV[1]
local dtutcnow = tonumber(ARGV[2])
local wait_base = tonumber(ARGV[3])
local wait_exp = tonumber(ARGV[4])
if wait_base == nil then wait_base = 3600 end
if wait_exp == nil then wait_exp = 2 end

local kworking   = ns .. sep .. "WORKING"   -- Jobs that have been consumed
local kworkers   = ns .. sep .. "WORKERS"   -- Worker IDs
local kfailed    = ns .. sep .. "FAILED"    -- Failed Queue
local kfailed    = ns .. sep .. "SCHEDULED" -- Scheduled Queue
local kmaxjobs   = ns .. sep .. "MAXJOBS"   -- Max number of jobs allowed
local kmaxfailed = ns .. sep .. "MAXFAILED" -- Max number of failures allowed

local kjob = ns .. sep .. "JOBS" .. sep .. jobid
local result

result = redis.pcall("ZSCORE", kworking, jobid)
if result == nil or is_error(result) then
    log_warn("Provided job not found. Job ID: " .. jobid .. "Queue: " .. kworking)
    return redis.error_reply("UNKNOWN_JOB_ID" .. " Job not found in queue." .. kworking)
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
    -- Backoff Sequence
    local nexttime = dtutcnow + (wait_base * (failures^wait_exp))
    result = redis.pcall("ZADD", kscheduled, nexttime, jobid);
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
