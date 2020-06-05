local M = {
    API_VERSION = 620,
    --cluster_config = "erlfdbtest:erlfdbtest@127.0.0.1:50681"
    --cluster_config = "./erlfdb.cluster"
    cluster_config = "/usr/local/etc/foundationdb/fdb.cluster"
}

-- mutex notes
-- from: https://stackoverflow.com/questions/34936019/pthread-mutex-not-working-correctly
-- Linux: #define PTHREAD_MUTEX_INITIALIZER { { 0, 0, 0, 0, 0, { 0 } } }
-- OSX:
--   #define PTHREAD_MUTEX_INITIALIZER {_PTHREAD_MUTEX_SIG_init, {0}}
--   #define _PTHREAD_MUTEX_SIG_init     0x32AAABA7

M.cdefs = [[
// fdb defs
typedef int fdb_error_t;
typedef int fdb_bool_t;
typedef struct FDB_database FDBDatabase;
typedef struct FDB_future FDBFuture;
typedef struct FDB_transaction FDBTransaction;

const char* fdb_get_error( fdb_error_t code );
fdb_error_t fdb_select_api_version_impl(int runtime_version, int header_version);
fdb_error_t fdb_run_network();
fdb_error_t fdb_setup_network();
fdb_error_t fdb_create_database(const char* cluster_file_path, FDBDatabase** out_database);
void fdb_database_destroy(FDBDatabase* database);
fdb_error_t fdb_database_create_transaction(FDBDatabase* database, FDBTransaction** out_transaction);
void fdb_transaction_destroy(FDBTransaction* transaction);
FDBFuture* fdb_transaction_commit(FDBTransaction* transaction);
fdb_error_t fdb_future_block_until_ready(FDBFuture* future);
fdb_bool_t fdb_future_is_ready(FDBFuture* future);
FDBFuture* fdb_transaction_on_error(FDBTransaction* transaction, fdb_error_t error);
void fdb_future_destroy(FDBFuture* future);
FDBFuture* fdb_transaction_get(FDBTransaction* transaction, uint8_t const* key_name, int key_name_length, fdb_bool_t snapshot);
void fdb_transaction_set(FDBTransaction* transaction, uint8_t const* key_name, int key_name_length, uint8_t const* value, int value_length);
fdb_error_t fdb_future_get_value(FDBFuture* future, fdb_bool_t* out_present, uint8_t const** out_value, int* out_value_length);
fdb_error_t fdb_future_get_error(FDBFuture* future);

// pthread defs
typedef unsigned long int pthread_t;

union pthread_attr_t
{
  char __size[64];
  long int __align;
};
typedef union pthread_attr_t pthread_attr_t;

int pthread_create(pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine) (void *), void *arg);
int pthread_join(pthread_t thread, void **value_ptr);
int pthread_cancel(pthread_t thread);

// Global defs
//pthread_mutex_t lock = {0x32AAABA7, {0}};
//const pthread_cond_t cond;

// Lua defs
static const int LUA_GLOBALSINDEX = -10002;
typedef struct lua_State lua_State;
typedef ptrdiff_t lua_Integer;
const char *(luaL_checklstring) (lua_State *L, int numArg, size_t *l);
lua_State *(luaL_newstate) (void);
int (luaL_error) (lua_State *L, const char *fmt, ...);
int (luaL_loadstring) (lua_State *L, const char *s);
void luaL_openlibs(lua_State *L);
void (lua_getfield) (lua_State *L, int idx, const char *k);
int (lua_pcall) (lua_State *L, int nargs, int nresults, int errfunc);
const char *(lua_tolstring) (lua_State *L, int idx, size_t *len);
lua_Integer lua_tointeger(lua_State *L, int idx);
void lua_settop(lua_State *L, int idx);
void lua_close (lua_State *L);
]]

M.setup_ffi = function(ffi)
    assert(ffi)
    print("SETTING CDEFS")
    ffi.cdef(M.cdefs)
end

local function error_to_string(error_num, ffi, fdb)
    assert(error_num and ffi and fdb)
    return ffi.string(fdb.fdb_get_error(error_num))
end
M.error_to_string = error_to_string

local function select_api_version(ffi, fdb, version)
    assert(ffi and fdb)
    version = version or M.API_VERSION
    -- NOTE: we're using fdb_select_api_version_impl as specified in:
    -- https://apple.github.io/foundationdb/api-c.html#c.fdb_select_api_version
    local error_num = fdb.fdb_select_api_version_impl(version, version)
    if error_num ~= 0 then
        local error_msg = error_to_string(error_num, ffi, fdb)
        local msg = string.format("Failed to set API version: %s", error_msg)
        assert(error_num == 0, msg)
    end
end
M.select_api_version = select_api_version

local function create_database(ffi, fdb, cluster_config)
    assert(fdb)
    cluster_config = clusterfile or M.cluster_config
    local fdb_db_ptr = ffi.new("FDBDatabase*[1]")
    local fdb_db_ptr_ptr = ffi.cast("FDBDatabase**", fdb_db_ptr)
    local err = fdb.fdb_create_database(cluster_config, fdb_db_ptr_ptr)
    if err ~= 0 then
        local err_msg = error_to_string(err, ffi, fdb)
        local msg = string.format("Failed to create database: %s", err_msg)
        assert(err == 0, msg)
    end

    return fdb_db_ptr
end
M.create_database = create_database

local function destroy_database(fdb, fdb_db)
    assert(fdb and fdb_db)
    fdb.fdb_database_destroy(fdb_db[0])
end
M.destroy_database = destroy_database

local function create_transaction(ffi, fdb, fdb_db)
    assert(ffi and fdb and fdb_db)
    print("Creating transaction")
    local fdb_tx_ptr = ffi.new("FDBTransaction*[1]")
    local fdb_tx_ptr_ptr = ffi.cast("FDBTransaction**", fdb_tx_ptr)

    local err = fdb.fdb_database_create_transaction(fdb_db[0], fdb_tx_ptr_ptr)
    if err ~= 0 then
        local err_msg = error_to_string(err, ffi, fdb)
        local msg = string.format("Failed to create transaction: %s", err_msg)
        assert(err == 0, msg)
    end

    return fdb_tx_ptr
end
M.create_transaction = create_transaction

local function destroy_transaction(fdb, fdb_tx)
    assert(fdb and fdb_tx)
    fdb.fdb_transaction_destroy(fdb_tx[0])
end
M.destroy_transaction = destroy_transaction

local function destroy_future(fdb, future)
    assert(fdb and future)
    print("Destroying future")
    fdb.fdb_future_destroy(future)
end
M.destroy_future = destroy_future

local function wait(ffi, fdb, future)
    assert(fdb and future)
    print("Waiting on transaction")
    local err = fdb.fdb_future_block_until_ready(future)
    if err ~= 0 then
        local err_msg = error_to_string(err, ffi, fdb)
        local msg = string.format("Failed to wait on transaction: %s", err_msg)
        assert(err == 0, msg)
    end
    destroy_future(fdb, future)
end
M.wait = wait

local function wait_val(ffi, fdb, future)
    assert(fdb and future)
    print(string.format("Waiting on transaction for value: %s", future))
    local err = fdb.fdb_future_block_until_ready(future)
    if err ~= 0 then
        local err_msg = error_to_string(err, ffi, fdb)
        local msg = string.format("Failed to wait on transaction for value: %s", err_msg)
        destroy_future(fdb, future)
        assert(err == 0, msg)
    end
    --print("Got future response")
    local out_present = ffi.new("fdb_bool_t[1]")
    local out_value = ffi.new("uint8_t const*[1]")
    local out_len = ffi.new("int[1]")
    err = fdb.fdb_future_get_value(future, out_present, out_value, out_len)
    destroy_future(fdb, future)
    --print("Got future value")
    if err ~= 0 then
        local err_msg = error_to_string(err, ffi, fdb)
        local msg = string.format("Failed to wait on transaction for value: %s", err_msg)
        assert(err == 0, msg)
    end
    print(string.format("GOT VAL{? %s}[%s]: %s", out_present[0], out_len[0], ffi.string(out_value[0])))
    if out_present[0] then
        return out_value[0]
    else
        print("   returning nil")
        return nil
    end
end
M.wait_val = wait_val

local function commit(fdb, tx)
    assert(fdb and tx)
    return fdb.fdb_transaction_commit(tx[0])
end
M.commit = commit

local function is_ready(fdb, future)
    assert(fdb and future)
    -- todo: have this return bool not fdb_bool_t
    return fdb.future_is_ready(future)
end
M.is_ready = is_ready

local function on_error(fdb, tx, err)
    assert(fdb and tx and err)
    return fdb.fdb_transaction_on_error(tx, err)
end
M.is_ready = on_error

local function do_transaction(ffi, fdb, tx, fun, wait_fun)
    wait_fun = wait_fun or wait
    assert(fdb and tx and fun)
    local success, res = pcall(fun, tx)
    if success then
        return wait_fun(ffi, fdb, commit(fdb, tx))
    else
        print(string.format("GOT TRANSACTION ERROR: %s", res))
        -- todo: need res to be of type fdb_error_t
        --wait(fdb, on_error(fdb, tx, res))
        --do_transaction(fdb, tx, fun)
    end
end
M.do_transaction = do_transaction

local function transactional(ffi, fdb, fdb_db, fun, wait_fun)
    local tx = create_transaction(ffi, fdb, fdb_db)
    return do_transaction(ffi, fdb, tx, fun, wait_fun)
end
M.transactional = transactional

local function setup_network(ffi, fdb)
    print("Setting up network")

    local err = fdb.fdb_setup_network()
    if err ~= 0 then
        print(string.format("{%s} FDB.FDB_SETUP_NETWORK[%s]: %s", unique_id, err, lfdbL.error_to_string(err, ffi, fdb)))
    end
    assert(err == 0, string.format("Unexpected fdb_setup_network error: %s", err))
end
M.setup_network = setup_network

local function run_network(ffi, fdb)
    print("Running fdb_run_network")
    local err = fdb.fdb_run_network()
    if err ~= 0 then
        print(string.format("{%s} FDB.FDB_RUN_NETWORK[%s]: %s", unique_id, err, lfdbL.error_to_string(err, ffi, fdb)))
    end
    assert(err == 0, string.format("Unexpected fdb_run_network error: %s", err))
    print("fdb_run_network complete")

    return ffi.cast("void**", err)
end
M.run_network = run_network

local function tx_get(fdb, tx, key)
    print(string.format("GOT KEY{%s}: %s", type(key), key))
    print(string.format("    TX: %s", tx[0]))
    local snapshot = false
    assert(fdb and tx and key)
    return fdb.fdb_transaction_get(tx[0], key, key:len(), snapshot)
end
M.tx_get = tx_get

local function tx_set(fdb, tx, key, val)
    print(string.format("SETTING KEY{%s}: %s -- VAL: %s", type(key), key, val))
    print(string.format("    TX: %s", tx[0]))
    assert(fdb and tx and key and val)
    return fdb.fdb_transaction_set(tx[0], key, key:len(), val, val:len())
end
M.tx_set = tx_set

local function db_get(ffi, fdb, fdb_db, key)
    return transactional(ffi, fdb, fdb_db, function(tx)
        return wait_val(ffi, fdb, tx_get(fdb, tx, key))
    end)
    --end, wait_val)
end
M.db_get = db_get

local function db_set(ffi, fdb, fdb_db, key, val)
    return transactional(ffi, fdb, fdb_db, function(tx)
        return tx_set(fdb, tx, key, val)
    end)
end
M.db_set = db_set

return M

