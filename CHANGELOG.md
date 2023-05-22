# 0.3.17 [unreleased]
- feat: Added Ipfs::pubsub_events to receive subscribe and unsubscribe events to a subscribed topic [PR 70]

[PR 70]: https://github.com/dariusc93/rust-ipfs/pull/70

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