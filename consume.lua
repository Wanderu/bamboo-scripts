--[[

consume <ns> , <client_name> <jobid> <datetime> <expires>

Keys: <ns>
    ns: Namespace under which queue data exists.
Args: <client_name> <jobid> <expires>
    client_name: The name/id of the worker.
    jobid: The Job ID to consume. Optional. Defaults to "". If not specified
            the next highest priority job is consumed.
    datetime: Current datetime (Unix seconds since epoch)
    expires: Seconds until Job expires and is candidate to go back on the queue.
            If "", defaults to 60 seconds.

Returns: Job data
Errors: MAXJOBS_REACHED, INVALID_CLIENT_NAME, NO_ITEMS, UNKNOWN_JOB_ID

-- ]]

local fname = "consume"
local sep = ":"

local log_notice = function (message)
    redis.log(redis.LOG_NOTICE, "<" .. fname .. ">" .. " " .. message)
end

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
local kqueue        = ns .. sep .. "QUEUED"   -- Primary Job queue
local kworking      = ns .. sep .. "WORKING"  -- Jobs that have been consumed
local kmaxjobs      = ns .. sep .. "MAXJOBS"  -- Max number of jobs allowed
local kworkers      = ns .. sep .. "WORKERS"  -- Worker IDs

local client_name = ARGV[1]
local jobid       = ARGV[2]

local dtutcnow = tonumber(ARGV[3])
if dtutcnow == nil then
    return redis.error_reply("INVALID_PARAMETER: datetime")
end

local expires = tonumber(ARGV[4])
if expires == "" or expires == nil then expires = 60 end

local max_jobs = tonumber(redis.pcall("GET", kmaxjobs))
local njobs = tonumber(redis.pcall("ZCARD", kworking))
local result

-- ######################
-- Don't consume more than the max number of jobs
-- ######################
if njobs and max_jobs and (njobs >= max_jobs) then
    return redis.error_reply("MAXJOBS_REACHED")
end

-- ######################
-- The client name must be non-blank.
-- Blank indicates initial job state with no owner.
-- ######################
if client_name == "" then
    log_warn("Client name not specified.")
    return redis.error_reply("INVALID_CLIENT_NAME")
end

-- ######################
-- If no jobid specified, then take the first item in the queue.
-- If a jobid was specified, find that job in the queue.
-- ######################
local score
if jobid == "" then
    -- peek at 1 item from the queue
    result = redis.call("ZRANGE", kqueue, 0, 0, 'withscores')

    if table.getn(result) == 0 then
        log_verbose("No items on the queue: " .. kqueue)
        return redis.error_reply("NO_ITEMS")
    end

    -- res is a table of length 2 with the item in the queue and the
    -- score/priority
    jobid = result[1]
    score = tonumber(result[2])
else
    result = redis.pcall("ZSCORE", kqueue, jobid)
    if is_error(result) then
        log_error(fname, "Cannot obtain score for job: " .. jobid ..
                "from zset: " .. kqueue);
        return redis.error_reply("UNKNOWN_JOB_ID")
    end
    score = tonumber(result)
end

-- We have a jobid by now. Make it's redis key.
local kjob = ns .. sep .. "JOBS" .. sep .. jobid

-- ######################
-- Pop Job from Main Queue
-- ######################
local nremoved = redis.call("ZREM", kqueue, jobid)

-- ######################
-- Add Job to Working Set
-- ######################
result = redis.pcall("ZADD", kworking, score, jobid)

-- ######################
-- Update Job state
-- ######################
result = redis.pcall("HMSET", kjob,
                     "owner", client_name,
                     "state", "working",
                     "consumed", dtutcnow);

-- ######################
-- Register this worker as a current worker and register the jobid with this
-- worker.  Expire the worker after a certain amount of time.
-- ######################
result = redis.pcall("SADD", kworkers, client_name) -- Set of worker names
local kworker = kworkers .. sep .. client_name
result = redis.pcall("SADD", kworker, jobid) -- Set of jobs that a worker has
-- Make a key for the worker that expires.
local kworker_active = kworkers .. sep .. client_name .. sep .. "ACTIVE"
redis.call("SET", kworker_active, 1) -- Key to signify that the worker is active.
redis.call("EXPIRE", kworker_active, expires)

-- NOTE: Expired jobs are those jobs in the working queue (and/or whose `state`
-- is set to "working") and has an owner (worker) that has expired.  A worker
-- whose kworker_active key has expired is considered no longer active. Any
-- outstanding Jobs belonging to that worker should be reaped and re-queued.

-- ######################
-- Return the Job data
-- ######################
return redis.call('HGETALL', kjob)
