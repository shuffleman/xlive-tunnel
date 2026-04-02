xlive-tunnel 传输层架构与双协议下行（HTTP-FLV + RTMP）支持

概览
- 上行：RTMP 推流接收，做握手、命令阶段（connect/createStream/publish）, 读取音视频消息，基于首个音频帧选择解密器，输出解密后的字节流。
- 下行（已有）：HTTP-FLV 分发，写 FLV Header + metadata，后续把加密后的数据写入 Video Tag。
- 下行（新增）：RTMP 拉流（play）分发，提供 PlayServer，在收到 connect/createStream/play 后发送 onStatus/metadata/编码配置，再把加密后的数据按音视频消息写出；提供 PlayClient 用于拉取并解封装还原明文。
- 密钥/加密：使用 crypto 包 CFB 流，UUID 或 SharedSecret 派生 Key/IV；上行 enc、下行 dec 成对使用。

关键模块
- rtmp.Server：[rtmp/server.go](file:///d:/Develooping/sing/xlive-tunnel/rtmp/server.go) 上行接入与解密出明文
- httpflv.ServerStream：[httpflv/server.go](file:///d:/Develooping/sing/xlive-tunnel/httpflv/server.go) 写 HTTP-FLV
- rtmp.PlayServer（新增）：[rtmp/play_server.go](file:///d:/Develooping/sing/xlive-tunnel/rtmp/play_server.go) RTMP 播放侧服务
- rtmp.PlayClient（新增）：[rtmp/play_client.go](file:///d:/Develooping/sing/xlive-tunnel/rtmp/play_client.go) RTMP 拉流客户端
- xlive.NewClient 选择下行协议：默认为 HTTP-FLV；设置 DownloadProto=rtmp 使用 RTMP 下行 [client_dial.go](file:///d:/Develooping/sing/xlive-tunnel/client_dial.go)

使用方式（库集成）
- 上行接入
  - 外部监听 TCP，Accept 后调用 rtmp.NewServer(conn, ServerOptions{SelectDecryptor})，Start 后从返回的 net.Conn 读取明文。
- 下行 HTTP-FLV
  - 在已有 HTTP 连接上创建 httpflv.NewServerStream(w, enc)，Start 后持续 Write(plaintext)。
- 下行 RTMP
  - 监听 TCP，Accept 后创建 rtmp.NewPlayServer(conn, PlayServerOptions{Enc, StreamName})，Start 后持续 Write(plaintext)。

协议选择策略（示例）
- HTTP 路由场景：通过 URL 参数或请求头决定调用 HTTP-FLV 还是 RTMP
  - URL 参数：?proto=flv 或 ?proto=rtmp
  - 请求头：X-Stream-Proto: flv|rtmp
  - 选择 flv 时构造 httpflv.ServerStream；选择 rtmp 时走独立 RTMP 端口或转发至 RTMP PlayServer。
- 客户端拨号（xlive.NewClient）
  - 默认 HTTP-FLV：设置 DownloadHost/DownloadPath
  - 指定 RTMP：设置 DownloadProto=rtmp，并提供 RTMP 的 net.Conn

测试与验证
- 单元测试
  - rtmp/play_server_test.go：验证 PlayServer 基本握手/播放与明文一致性
  - client_dial_rtmp_test.go：xlive.NewClient 下行 RTMP 路径覆盖
- 并发与双协议
  - dual_protocol_test.go：同源明文并行推送到 HTTP-FLV 与 RTMP，双端拉取输出一致
- 运行
  - 在仓库根目录执行 go test ./... 

部署建议
- 典型拓扑：上行 RTMP 接入 → 内部总线（明文字节流）→ 下行分发模块（HTTP-FLV 与 RTMP Play 并行）
- 独立扩容：HTTP 与 RTMP 下行实例分别横向扩容；两者仅依赖同一明文输入
- 端口规划：HTTP 走现有服务；RTMP 单独监听 TCP 1935 或内部端口

性能基准（建议基线）
- 单连接配置 ChunkSize=262144，服务端 WindowAckSize=2.5MB
- 1k 并发连接下行，64KB 块大小，RTMP 与 HTTP-FLV 吞吐接近链路上限，CPU 主要消耗在加解密与内存拷贝
- 根据业务环境建议使用多核绑核与预分配缓冲

故障排查
- 无法播放：检查握手与命令流程（connect/_result、createStream/_result、play/onStatus）
- 花屏/卡顿：确认音频首帧到达并完成密钥选择；检查加解密两端 Key/IV 一致
- HTTP-FLV 无法打开：确认响应头与 FLV Header 已写入；确认流式发送未被代理缓冲
- RTMP 间歇中断：观察 ACK 窗口、ChunkSize 设置与网络超时时间

配置示例片段
- RTMP 上行选择解密器：func([]byte) (cipher.Stream, error) 返回 CFB 解密流
- HTTP-FLV：httpflv.NewServerStream(w, enc)
- RTMP Play：rtmp.NewPlayServer(conn, PlayServerOptions{Enc: enc, StreamName: "live_test"})
