#!lua name=turbodiesel

local function td_set(keys, args)
  local key = keys[1]
  local value = args[1]
  local input_sec = tonumber(args[2])
  local input_nsec = tonumber(args[3])

  local invalidate_ts = redis.call("HMGET", key, 'inv_sec', 'inv_nsec')
  local inv_sec = tonumber(invalidate_ts[1]) or 0
  local inv_nsec = tonumber(invalidate_ts[2]) or 0

  if input_sec < inv_sec or (input_sec == inv_sec and input_nsec < inv_nsec) then
    return 0 -- Skipped (data might be stale)
  else
    redis.call("HSET", key, 'ts_sec', input_sec, 'ts_nsec', input_nsec, 'v', value)
    return 1
  end
end

redis.register_function('td_set', td_set)

local function td_invalidate(keys, args)
  local key = keys[1]
  local input_sec = tonumber(args[1])
  local input_nsec = tonumber(args[2])

  local invalidate_ts = redis.call("HMGET", key, 'inv_sec', 'inv_nsec')
  local inv_sec = tonumber(invalidate_ts[1]) or 0
  local inv_nsec = tonumber(invalidate_ts[2]) or 0

  if input_sec < inv_sec or (input_sec == inv_sec and input_nsec < inv_nsec) then
    return 0 -- Skipped (existing invalidation is newer than the one requested)
  else
    redis.call("HSET", key, 'inv_sec', input_sec, 'inv_nsec', input_nsec)
    redis.call("EXPIRE", key, 120)
    return 1
  end
end

redis.register_function('td_invalidate', td_invalidate)

local function td_get(keys, args)
  local key = keys[1]

  local record = redis.call("HMGET", key, 'ts_sec', 'ts_nsec', 'inv_sec', 'inv_nsec', 'v')
  if record[5] == nil then
    return nil -- Not in cache
  end
  local ts_sec = tonumber(record[1]) or 0
  local ts_nsec = tonumber(record[2]) or 0
  local inv_sec = tonumber(record[3]) or 0
  local inv_nsec = tonumber(record[4]) or 0
  local value = record[5]

  if ts_sec < inv_sec or (ts_sec == inv_sec and ts_nsec < inv_nsec) then
    return nil -- invalidated
  else
    return value
  end
end

redis.register_function('td_get', td_get)
