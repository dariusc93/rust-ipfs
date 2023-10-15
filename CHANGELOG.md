# 0.6.0 [unreleased]
- refactor: Disable primary protocols by default [PR 109]  
- refactor: Move repo logic into a single task [PR 108]

[PR 109]: https://github.com/dariusc93/rust-ipfs/pull/109
[PR 108]: https://github.com/dariusc93/rust-ipfs/pull/108

# 0.5.0
- feat: Implement timeout while resolving blocks [PR 106]

[PR 106]: https://github.com/dariusc93/rust-ipfs/pull/106

# 0.4.6
- chore: Updated libp2p-relay-manager

# 0.4.5
- chore: Add json and cbor feature

# 0.4.4
- chore: Use new method over deprecated and internal behaviours [PR 104]
- fix: Set dns resolver as a field, and fix resolved path [PR 103]
- chore: Remove experimental feature flag

[PR 104]: https://github.com/dariusc93/rust-ipfs/pull/104
[PR 103]: https://github.com/dariusc93/rust-ipfs/pull/103

# 0.4.3
- feat: Implement rendezvous protocol [PR 101]
- chore: Cleanup future task [PR 102]

[PR 101]: https://github.com/dariusc93/rust-ipfs/pull/101
[PR 102]: https://github.com/dariusc93/rust-ipfs/pull/102

# 0.4.2
- feat: Implemented relay manager [PR 96]
- feat: Implement idle connections timeout [PR 98]
- chore: Add UninitializedIpfs::set_listening_addrs and minor changes
- chore: Add peer to dht when discovered over mdns

[PR 96]: https://github.com/dariusc93/rust-ipfs/pull/96
[PR 98]: https://github.com/dariusc93/rust-ipfs/pull/98

# 0.4.1
-  fix: Dont close connections on ping error [PR 95]

[PR 95]: https://github.com/dariusc93/rust-ipfs/pull/95

# 0.4.0
- chore: Uncomment and update relay configuration [PR 94]
- chore: Minor Optimizations [PR 93]
- refactor: Make `Repo` independent [PR 92]
- feat: Basic record key/prefix validator and basic ipns publisher/resolver [PR 88]
- chore: Update dependencies [PR 91]
- chore: Update libp2p to 0.52 [PR 76]
- chore: Add `UninitializedIpfs::listen_as_external_addr` to use listened addresses as external addresses [PR 90]

[PR 94]: https://github.com/dariusc93/rust-ipfs/pull/94
[PR 93]: https://github.com/dariusc93/rust-ipfs/pull/93
[PR 92]: https://github.com/dariusc93/rust-ipfs/pull/92
[PR 88]: https://github.com/dariusc93/rust-ipfs/pull/88
[PR 91]: https://github.com/dariusc93/rust-ipfs/pull/91
[PR 76]: https://github.com/dariusc93/rust-ipfs/pull/76
[PR 90]: https://github.com/dariusc93/rust-ipfs/pull/90

# 0.3.19
- refactor: Update libipld and switch to using quick-protobuf [PR 87]

[PR 87]: https://github.com/dariusc93/rust-ipfs/pull/87

# 0.3.18
- fix: Add Sync + Send to custom transport signature

# 0.3.17
- feat: Support custom transport [PR 84]
- chore: Implement functions to add session to bitswap [PR 83]
- refactor: Use channels instead of Subscription [PR 82]
- feat: Ability to add custom behaviour [PR 81]
- feat: Implement Ipfs::connection_events [PR 80]
- chore: Remove the initial notify and add Ipfs::listener_addresses and Ipfs::external_addresses [PR 79]
- fix: Properly emit pubsub event of a given topic [PR 77]
- refactor: Confirm event from swarm when disconnecting from peer [PR 75]
- feat: Implement Keystore [PR 74]
- feat: Added Ipfs::remove_peer and Ipfs::remove_peer_address [PR 73]
- feat: Implements MultiaddrExt and remove wrapper [PR 72]
- feat: Basic AddressBook implementation [PR 71]
- feat: Added Ipfs::pubsub_events to receive subscribe and unsubscribe events to a subscribed topic [PR 70]
- feat: Implement RepoProvider [PR 69]
- refactor: Switch bitswap implementation [PR 66]

[PR 66]: https://github.com/dariusc93/rust-ipfs/pull/66
[PR 69]: https://github.com/dariusc93/rust-ipfs/pull/69
[PR 70]: https://github.com/dariusc93/rust-ipfs/pull/70
[PR 71]: https://github.com/dariusc93/rust-ipfs/pull/71
[PR 72]: https://github.com/dariusc93/rust-ipfs/pull/72
[PR 73]: https://github.com/dariusc93/rust-ipfs/pull/73
[PR 74]: https://github.com/dariusc93/rust-ipfs/pull/74
[PR 75]: https://github.com/dariusc93/rust-ipfs/pull/75
[PR 77]: https://github.com/dariusc93/rust-ipfs/pull/77
[PR 79]: https://github.com/dariusc93/rust-ipfs/pull/79
[PR 80]: https://github.com/dariusc93/rust-ipfs/pull/80
[PR 81]: https://github.com/dariusc93/rust-ipfs/pull/81
[PR 82]: https://github.com/dariusc93/rust-ipfs/pull/82
[PR 83]: https://github.com/dariusc93/rust-ipfs/pull/83
[PR 84]: https://github.com/dariusc93/rust-ipfs/pull/84

# 0.3.16
- fix: Return events from gossipsub stream [PR 68]
- chore: Update transport and configuration

[PR 68]: https://github.com/dariusc93/rust-ipfs/pull/68

# 0.3.15
- fix: Remove item from want list [PR 64] 
- chore: Impl sled datastore, split stores into own modules [PR 63]
- feat: Added AddOption::wrap [PR 62]
- chore: Update configuration and behaviour cleanup [PR 60]
- chore: Update libp2p to 0.51.3 [PR 61]

[PR 60]: https://github.com/dariusc93/rust-ipfs/pull/60
[PR 61]: https://github.com/dariusc93/rust-ipfs/pull/61
[PR 62]: https://github.com/dariusc93/rust-ipfs/pull/62
[PR 63]: https://github.com/dariusc93/rust-ipfs/pull/63
[PR 64]: https://github.com/dariusc93/rust-ipfs/pull/64

# 0.3.14
- chore: Downgrade libp2p-gossipsub (temporarily)

# 0.3.13
- chore: Remove condition that prevents storing rtt [PR 58]
- chore: Reduce allocation in bitswap and pubsub [PR 59]

[PR 58]: https://github.com/dariusc93/rust-ipfs/pull/58
[PR 59]: https://github.com/dariusc93/rust-ipfs/pull/59

# 0.3.12
- chore: Update libp2p to 0.51.2 and add message_id_fn

# 0.3.11
- chore: Lock libp2p to 0.51.1

# 0.3.10
- chore: Reexport transport items

# 0.3.9
- fix: Use peer_connections for peers function [PR 54]
- chore(repo): Added field to only check locally [PR 55]
- refactor: Remove Column from DataStore [PR 56]
- feat: Add IpfsUnixfs [PR: 57]

[PR 54]: https://github.com/dariusc93/rust-ipfs/pull/54
[PR 55]: https://github.com/dariusc93/rust-ipfs/pull/55
[PR 56]: https://github.com/dariusc93/rust-ipfs/pull/56
[PR 57]: https://github.com/dariusc93/rust-ipfs/pull/57

# 0.3.8
- chore: Wait on identify before returning connection [PR 47]
- feat(repo): Allow custom repo store [PR 46]
- chore: Make kademlia optional [PR 45]
- chore: Make mplex optional [PR 51]
- refactor: Provide peers when obtaining blocks [PR 52]
- refactor: Remove Keypair from IpfsOptions [PR 53]
- refactor: Remove SwarmApi [PR 50]

[PR 47]: https://github.com/dariusc93/rust-ipfs/pull/47
[PR 46]: https://github.com/dariusc93/rust-ipfs/pull/46
[PR 45]: https://github.com/dariusc93/rust-ipfs/pull/45
[PR 51]: https://github.com/dariusc93/rust-ipfs/pull/51
[PR 52]: https://github.com/dariusc93/rust-ipfs/pull/52
[PR 53]: https://github.com/dariusc93/rust-ipfs/pull/53
[PR 50]: https://github.com/dariusc93/rust-ipfs/pull/50

# 0.3.7
- chore: Cleanup deprecation [PR 44]

[PR 44]: https://github.com/dariusc93/rust-ipfs/pull/44

# 0.3.6
- fix: Change condition when attempting to clear out cache

# 0.3.5
- fix: Export KadInserts

# 0.3.4
- refactor: Changes to connection and listening functions [PR 41]
- chore: Add KadConfig [PR 42]

[PR 41]: https://github.com/dariusc93/rust-ipfs/pull/41
[PR 42]: https://github.com/dariusc93/rust-ipfs/pull/42

# 0.3.3
- chore: Reenable relay configuration

# 0.3.2
- refactor: Use async-broadcast for pubsub stream [PR 39]

[PR 39]: https://github.com/dariusc93/rust-ipfs/pull/39

# 0.3.1
- chore: Minor Config for Pubsub [PR 38]

[PR 38]: https://github.com/dariusc93/rust-ipfs/pull/38

# 0.3.0
- chore: Update to libp2p 0.51.0 [PR 31]
This also re-enables quic-v1 that was disabled due to versioning issues after the release of 0.51.0
- refactor/transport: Simplify TransportConfig and added additional options [PR 32]
- feat: basic PeerBook implementation [PR 34]
- refactor(repo): Remove RepoTypes and IpfsTypes [PR 35]

[PR 31]: https://github.com/dariusc93/rust-ipfs/pull/31
[PR 32]: https://github.com/dariusc93/rust-ipfs/pull/32
[PR 34]: https://github.com/dariusc93/rust-ipfs/pull/34
[PR 35]: https://github.com/dariusc93/rust-ipfs/pull/35
# 0.3.0-alpha.6
- chore: Disable quic-v1 until 0.51 update

# 0.3.0-alpha.5
- fix/: Add debug derive to FDLimit

# 0.3.0-alpha.4
- chore/: Add small delay before processing events [PR 29]

[PR 29]: https://github.com/dariusc93/rust-ipfs/pull/29

# 0.3.0-alpha.3
- refactor/opt: Expanded UninitializedIpfs functionality [PR 21]
- feat/upnp: Use libp2p-nat for handling port forwarding [PR 23]
- refactor/task: Move swarm and events into a task [PR 25]
- chore/: Remove async from event handling functions [PR 27]
- refactor/subscription: Use channels instead of subscription [PR 28]

[PR 23]: https://github.com/dariusc93/rust-ipfs/pull/23
[PR 21]: https://github.com/dariusc93/rust-ipfs/pull/21
[PR 25]: https://github.com/dariusc93/rust-ipfs/pull/25
[PR 27]: https://github.com/dariusc93/rust-ipfs/pull/27
[PR 28]: https://github.com/dariusc93/rust-ipfs/pull/28

# 0.3.0-alpha.2
- fix: Poll receiving oneshot channel directly [PR 20]

[PR 20]: https://github.com/dariusc93/rust-ipfs/pull/20

# 0.3.0-alpha.1
- See commit history