#include "broker_config.h"

BrokerConfig::BrokerConfig(
    int port,
    int broker_id,
    std::string ip_address,
    std::vector<ClusterPeer> peers,
    std::string data_directory,
    BrokerType type,
    int leader_broker_id
)
    : port(port),
      broker_id(broker_id),
      ip_address(std::move(ip_address)),
      peers(std::move(peers)),
      data_directory(std::move(data_directory)),
      type(type),
      leader_broker_id(leader_broker_id) {}

bool BrokerConfig::get_leader_endpoint(LeaderEndpoint& endpoint) const {
    if (leader_broker_id == broker_id) {
        endpoint = LeaderEndpoint{broker_id, ip_address, port};
        return true;
    }

    for (const ClusterPeer& peer : peers) {
        if (peer.broker_id == leader_broker_id) {
            endpoint = LeaderEndpoint{peer.broker_id, peer.host, peer.port};
            return true;
        }
    }

    return false;
}
