local lfdbL = require "lfdb_lib"
local ffi = require "ffi"

lfdbL.setup_ffi(ffi)

local C = ffi.C
local fdb = ffi.load("fdb_c")
local pthread = ffi.load("pthread")

local unique_id = tostring( {} ):sub(8)

local chunk = [[
local unique_id = tostring( {} ):sub(8)
local lfdbL = require "lfdb_lib"
local ffi = require "ffi"
lfdbL.setup_ffi(ffi)
local fdb = ffi.load("fdb_c")

print(string.format("{%s} Spawned thread: {%s}", unique_id, unique_id))

local function fdb_network_thread()
    return lfdbL.run_network(ffi, fdb)
end

cb_fdb_network_thread = tonumber(ffi.cast('intptr_t', ffi.cast('void *(*)(void *)', fdb_network_thread)))
]]

local function spawn_thread()
    local pid = ffi.new("pthread_t[1]")
    local L = assert(C.luaL_newstate())
    C.luaL_openlibs(L)
    local res = C.luaL_loadstring(L, chunk)
    assert(res == 0)
    --res = C.lua_pcall(L, 0, 1, 0)
    res = C.lua_pcall(L, 0, 0, 0)
    if res ~= 0 then
        print(string.format("thread error[%s]: %s", unique_id, ffi.string(C.lua_tolstring(L, -1, nil))))
    end
    assert(res == 0)

    C.lua_getfield(L, C.LUA_GLOBALSINDEX, "cb_fdb_network_thread")
    local fun = C.lua_tointeger(L, -1)
    print(string.format("THREAD FUN IS: %s", fun))
    C.lua_settop(L, -2)

    print(string.format("{%s} THREAD CREATING", unique_id))
    res = pthread.pthread_create(pid, nil, ffi.cast('void *(*)(void *)', fun), nil)
    print(string.format("{%s} THREAD CREATED", unique_id))
    assert(res == 0)

    return pid, L
end

lfdbL.select_api_version(ffi, fdb)
lfdbL.setup_network(ffi, fdb)
print(string.format("fdb.C.fdb_get_error(1): %s", lfdbL.error_to_string(1, ffi, fdb)))
print(string.format("{%s} INVOKING THE THREAD!", unique_id))
local thread, thread_L = spawn_thread()
print(string.format("{%s} DONE INVOKING THE THREAD!", unique_id))
print(string.format("THREAD IS: %s", thread[0]))

-- busy loop networking thread coordination hack
local t = 1
for i=1,100000 do t = t * i * math.log(t*3) end

print("CREATING FDB_DB")
local fdb_db = lfdbL.create_database(ffi, fdb)
print(string.format("FDB_DB IS: %s", fdb_db))

local fdb_tx = lfdbL.create_transaction(ffi, fdb, fdb_db)
print(string.format("FDB_TX IS: %s", fdb_tx))
lfdbL.do_transaction(ffi, fdb, fdb_tx, function(tx) print("IN TRANSACTION!"); assert(true) end)
lfdbL.destroy_transaction(fdb, fdb_tx)

lfdbL.transactional(ffi, fdb, fdb_db, function(tx) print("Failing the transaction!"); assert(false) end)

local key = "asdffdsa"
local val = lfdbL.db_get(ffi, fdb, fdb_db, key)
print(string.format("GOT VAL FOR KEY{%s}: %s", key, val))
lfdbL.db_set(ffi, fdb, fdb_db, key, "12344321")
val = lfdbL.db_get(ffi, fdb, fdb_db, key)
print(string.format("GOT VAL FOR KEY{%s}: %s", key, val))

print("Closing shop...")
local res = pthread.pthread_cancel(thread[0])
assert(res == 0)
lfdbL.destroy_database(fdb, fdb_db)
C.lua_close(thread_L)
print("Exiting...")

