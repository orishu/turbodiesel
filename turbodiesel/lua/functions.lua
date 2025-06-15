#!lua name=turbodiesel

local function td_set(keys, args)
  local key = keys[1]
  local value = args[1]
  local ts_sec = tonumber(args[2])
  local ts_nsec = tonumber(args[3])

  local invalidate_key = "invalidate:" .. key
  local invalidate_timestamp = redis.call("HGETALL", invalidate_key) -- Expected format: {'sec', seconds, 'nsec', nseconds}
  local inv_sec = tonumber(invalidate_timestamp[2]) or 0
  local inv_nsec = tonumber(invalidate_timestamp[4]) or 0

  if ts_sec < inv_sec or (ts_sec == inv_sec and ts_nsec < inv_nsec) then
    return 0 -- Skipped (data might be stale)
  else
    redis.call("HSET", key, 'ts_sec', ts_sec, 'ts_nsec', ts_nsec, 'v', value)
    return 1
  end
end

redis.register_function('td_set', td_set)

local function td_invalidate(keys, args)
  local key = keys[1]
  local sec = tonumber(args[1])
  local nsec = tonumber(args[2])

  local invalidate_key = "invalidate:" .. key
  local invalidate_timestamp = redis.call("HGETALL", invalidate_key) -- Expected format: {'sec', seconds, 'nsec', nseconds}
  local old_sec = tonumber(invalidate_timestamp[2]) or 0
  local old_nsec = tonumber(invalidate_timestamp[4]) or 0

  if sec < old_sec or (sec == old_sec and nsec < old_nsec) then
    return 0 -- Skipped (existing invalidation is newer than the one requested)
  else
    redis.call("HSET", invalidate_key, 'sec', sec, 'nsec', nsec)
    return 1
  end
end

redis.register_function('td_invalidate', td_invalidate)

local function td_get(keys, args)
  local key = keys[1]

  local record = redis.call("HGETALL", key) -- Expected format: {'ts_sec', seconds, 'ts_nsec', nseconds, 'v', value}
  local sec = tonumber(record[2])
  local nsec = tonumber(record[4])
  local value = record[6]

  local invalidate_key = "invalidate:" .. key
  local invalidate_timestamp = redis.call("HGETALL", invalidate_key) -- Expected format: {'sec', seconds, 'nsec', nseconds}
  local inv_sec = tonumber(invalidate_timestamp[2]) or 0
  local inv_nsec = tonumber(invalidate_timestamp[4]) or 0

  if sec < inv_sec or (sec == inv_sec and nsec < inv_nsec) then
    return nil -- invalidated
  else
    return value
  end
end

redis.register_function('td_get', td_get)

