#include "tsuba/NameServerClient.h"

#include "GlobalState.h"

tsuba::NameServerClient::~NameServerClient() = default;

void
tsuba::SetMakeNameServerClientCB(
    std::function<katana::Result<std::unique_ptr<tsuba::NameServerClient>>()>
        cb) {
  GlobalState::set_make_name_server_client_cb(cb);
}

void
tsuba::ClearMakeNameServerClientCB() {
  GlobalState::clear_make_name_server_client_cb();
}
