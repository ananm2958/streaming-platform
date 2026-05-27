#pragma once

#include <string>
#include <vector>

enum BrokerType { LEADER, FOLLOWER };

struct ClusterPeer {
    int broker_id;
    std::string host;
    int port;
};

struct LeaderEndpoint {
    int broker_id;
    std::string host;
    int port;
};

class BrokerConfig {
public:
    BrokerConfig(
        int port,
        int broker_id,
        std::string ip_address,
        std::vector<ClusterPeer> peers,
        std::string data_directory,
        BrokerType type,
        int leader_broker_id
    );

    int get_port() const { 
        return port; 
    }

    int get_broker_id() const { 
        return broker_id; 
    }
    
    const std::string& get_ip_address() const { 
        return ip_address; 
    }

    const std::vector<ClusterPeer>& get_peers() const { 
        return peers; 
    }

    const std::string& get_data_directory() const { 
        return data_directory; 
    }

    BrokerType get_type() const { 
        return type; 
    }
    
    int get_leader_broker_id() const { 
        return leader_broker_id; 
    }

    bool is_leader() const { 
        return type == LEADER; 
    }

    bool get_leader_endpoint(LeaderEndpoint& endpoint) const;

private:
    int port;
    int broker_id;
    std::string ip_address;
    std::vector<ClusterPeer> peers;
    std::string data_directory;
    BrokerType type;
    int leader_broker_id;
};
