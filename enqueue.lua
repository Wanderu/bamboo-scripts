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

local priority = tostring(ARGV[1])
local ipriority = tonumber(ARGV[1])
local jobid    = ARGV[2]

-- Primary Job queue (ZSet)
local kqueue  = ns .. sep .. "QUEUED"
-- Key of the Job data (HMap)
local kjob = ns .. sep .. "JOBS" .. sep .. jobid

local msg, result;
local argc = tonumber(table.getn(ARGV));

-- Make a table of all Job object parameters
local job_data = {};
local n = 0;
for i = 3, argc do
    table.insert(job_data, ARGV[i]);
    n = n+1
end

-- The number of key/value params
-- local n = tonumber(table.getn(job_data));

if (n % 2 == 1) then
    msg = "Invalid number of job object parameters: " .. tostring(n)
    log_warn(msg)
    return redis.error_reply("INVALID_PARAMETERS")
end

local current_score = tonumber(redis.call("zscore", kqueue, jobid));

local exists = redis.call("EXISTS", kjob)
if tonumber(exists) == 1 and current_score and ipriority >= current_score then
    msg = " Not enqueing item. "
          .. "An existing item has the same or lesser score.";
    log_warn(msg)
    return 0;
end

-- Make the Job object as a hash map.
redis.call("HMSET", kjob, unpack(job_data));
redis.call("HMSET", kjob, "priority", priority, "state", "enqueued");

-- Add Job ID to queue
result = redis.pcall("ZADD", kqueue, ipriority, jobid);
if is_error(result) then
    redis.call("DEL", kjob) -- Remove the Job (rollback)
    msg = "ZADD operation failed. result: " .. tostring(result);
    log_warn(msg)
    return redis.error_reply(msg)
end

return 1;
