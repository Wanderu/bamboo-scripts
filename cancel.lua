--[[

cancel <ns> , <jobid>

Keys:
    ns: Namespace under which queue data exists.

Args:
    jobid: Job identifier.

Returns: 1 if canceled correctly.
Errors: UNKNOWN_JOB_ID, JOB_IN_WORK

--]]

local fname = "cancel"
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

local kworking   = ns .. sep .. "WORKING"   -- Jobs that have been consumed
local kqueue     = ns .. sep .. "QUEUED"
local kscheduled = ns .. sep .. "SCHEDULED" -- Scheduled Queue
local kfailed    = ns .. sep .. "FAILED"
-- local kfailed    = ns .. sep .. "FAILED"    -- Failed Queue

local kjob = ns .. sep .. "JOBS" .. sep .. jobid
local exists = tonumber(redis.call("EXISTS", kjob))
local result

-- if it's in work, can't cancel
result = redis.call("ZSCORE", kworking, jobid)
-- result = redis.pcall("ZSCORE", kworking, jobid)
-- log_verbose(cjson.encode(result))
if result ~= nil then
    log_warn("Job already in work, cannot remove/cancel.")
    redis.error_reply("JOB_IN_WORK")
end

redis.call("ZREM", kqueue, jobid)
redis.call("ZREM", kscheduled, jobid)
redis.call("ZREM", kfailed, jobid)

if exists == nil then
    redis.error_reply("UNKNOWN_JOB_ID")
end

result = redis.call("DEL", kjob);
return result
