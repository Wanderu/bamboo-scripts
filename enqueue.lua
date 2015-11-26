--[[
-- enqueue
-- Keys: <ns>
--  ns: Namespace under which queue data exists.
-- Args: <priority> <jobid> <key> <val> [<key> <val> ...]
--       key, val pairs are values representing a Job object.
--
-- Returns: 0 if job already existed. 1 if added.
-- Errors: INVALID_PARAMETERS
-- ]]

local fname = "enqueue"
local sep = ":"

local log_warn = function (message)
    redis.log(redis.LOG_WARNING, "<" .. fname .. ">" .. " " .. message)
end

local log_notice = function (message)
    redis.log(redis.LOG_NOTICE, "<" .. fname .. ">" .. " " .. message)
end

local log_verbose = function (message)
    redis.log(redis.LOG_VERBOSE, "<" .. fname .. ">" .. " " .. message)
end

local is_error = function(result)
    return type(result) == 'table' and result.err
end

local ns = KEYS[1]

local priority = tonumber(ARGV[1])
local jobid = ARGV[2]

-- Primary Job queue (ZSet)
local kqueue  = ns .. sep .. "QUEUED"
local kscheduled  = ns .. sep .. "SCHEDULED"
local kworking   = ns .. sep .. "WORKING"  -- Jobs that have been consumed
-- Key of the Job data (HMap)
local kjob = ns .. sep .. "JOBS" .. sep .. jobid

local msg, result;

-- ######################
-- Make a table of all Job object parameters
-- ######################
local job_data = {}
local n = 0
for i = 3, tonumber(table.getn(ARGV)) do
    table.insert(job_data, ARGV[i])
    n = n+1
end

if (n % 2 == 1) then
    msg = "Invalid number of job object parameters: " .. tostring(n)
    log_warn(msg)
    return redis.error_reply("INVALID_PARAMETERS")
end

local exists = tonumber(redis.call("EXISTS", kjob))

-- ######################
-- Make/Update the Job object (hash map).
-- ######################
redis.call("HMSET", kjob, unpack(job_data))

-- ######################
-- If this job already existed, then we are updating it's queue status
-- ######################
if exists == 1 then
    -- ######################
    -- Check to see if the job is in the WORKING queue.
    -- If it is, quit with error.
    -- ######################
    if tonumber(redis.call("zscore", kworking, jobid)) ~= nil then
        log_warn("Job exists in WORKING queue. Job ID: " .. jobid)
        return redis.error_reply("JOB_IN_WORK")
    end

    -- ######################
    -- Check to see if the job is in the SCHEDULED queue.
    -- If it is, move it to the QUEUED queue.
    -- ######################
    if tonumber(redis.call("zscore", kscheduled, jobid)) ~= nil then
        log_notice("Item already exists in SCHEDULED queue. Moving to QUEUED queue.")
        redis.call("ZREM", kscheduled, jobid)
    end

    -- ######################
    -- Check if job is already queued.
    -- Only requeue if the priority is higher (lower score).
    -- ######################
    local current_score = tonumber(redis.call("zscore", kqueue, jobid))
    if current_score ~= nil and priority >= current_score then
        log_warn("Not enqueing item. An existing item has the same or lesser score.")
        return 0 -- return redis.error_reply("JOB_EXISTS")
    end

end

redis.call("HMSET", kjob, "priority", priority, "state", "enqueued")

-- ######################
-- Add Job ID to queue
-- ######################
redis.call("ZADD", kqueue, priority, jobid)

return 1;
