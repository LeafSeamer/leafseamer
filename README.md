# LeafSeamer / Seamer

> **施工中 / Work in progress。** 已有独立保护态进程、持久服务身份和本机认证时钟接线。本机一次性配对、配对后的人员身份核验及 GUI 草稿编辑已接通。首期自动化、跨主机和生产部署验收仍在进行。

自动化核心：规则、状态机、排期、能力引用和执行记录。通过公共 API 调用 Controller，不实现厂商设备协议。

总体设计、已确认决策、待落实事项和路线图统一维护于 [LeafSeamer Suite](https://github.com/LeafSeamer/leafseamer-suite)。已按确认的依赖顺序在 `main` 实施。

## 开发构建与运行

使用 Node 24.22.0、npm 11.19.1 和精确锁定依赖。原生时钟采用 CMake 与 C++17；Windows x64 使用 Visual Studio 2022，Linux x64 使用对应平台编译器。Node头文件及Windows链接库须匹配 `native-inputs.json`，不接受任意版本替代。

```text
npm ci --ignore-scripts
npm --prefix ../leafseamer-sdk run build
npm --prefix ../leafseamer-controller run build
npm run build:native -- --headers <Node头文件绝对目录> --node-library <Windows node.lib绝对路径>
npm run build
node --disallow-code-generation-from-strings dist/cli.mjs init-storage --data-dir <数据绝对目录>
node --disallow-code-generation-from-strings dist/cli.mjs start --data-dir <数据绝对目录> --port 0
```

Linux构建省略 `--node-library`。也可显式运行 `npm run build -- --native-artifacts <制品绝对目录>`，采用 `runtime-inputs.json` 中固定摘要的现有原生时钟制品。采用路径核验制品清单、构建时的源码和 CMake 摘要、平台、Node 版本及二进制，本机构建核验当前源码。完成构建后可用 `npm pack --ignore-scripts` 打包，安装包运行不依赖邻仓路径。

Linux 原生采集与 Controller 共用挂起及墙钟跳变检查。采集前后同步核验 `CLOCK_BOOTTIME` 和 `CLOCK_MONOTONIC` 的偏移，并检查绝对 `CLOCK_REALTIME` timerfd。发现确定的偏移变化、墙钟跳变或观察失效时，当次采集拒绝并在原生实例内锁存故障。单调读数仍供期限检查。活动部署电源策略、授时环境和认证资格还需独立验收。[Linux 时钟手册](https://man7.org/linux/man-pages/man2/clock_gettime.2.html) · [Linux timerfd 手册](https://man7.org/linux/man-pages/man2/timerfd_create.2.html)

初始化以独占创建生成Seamer自己的service_id、database_instance_id和request_epoch；已存在文件拒绝覆盖。SQLite固定引擎与来源，使用WAL、FULL同步、外键和排他连接。每次运行生成新boot，普通重启保留稳定身份。数据库版本、迁移摘要或完整性异常拒绝打开；进程异常退出保留未关闭boot历史，不推断任何运行已经安全完成。

普通CLI只监听回环地址。`GET /health/live` 表示进程存活；`GET /health/ready` 始终为503，区分认证时钟未就绪与业务尚未完成。普通HTTP没有初始化、登录、配对或自动化写入口。`identity-status` 是本机只读身份诊断，运行中的排他连接会拒绝第二实例。

认证时钟代码和原生采样来自同一已固定Controller运行库源码，具体来源和字节摘要见 `runtime-inputs.json`；构建核对这些副本，更新须显式更换固定版本。Seamer在自己的进程采样、在自己的SQLite保存原件，不能使用远端tick。可信本机装配可向 `startSeamer` 提供认证资格提供者；普通CLI、环境变量和HTTP不能注入批准正文。缺环境批准保持unqualified，健康或采集失效锁存保护，重启不会恢复旧域资格。认证时钟可用仍不意味着业务可用。

`test/storage.test.mjs`、`test/process.test.mjs`和`test/auth-clock.test.mjs`覆盖文件库、真实CLI进程、回环HTTP、排他拒绝、重启、实际原生采样及认证保护。测试中的环境批准为合成材料，不是平台资格；Linux运行、完整监督、独立原生重新编译、配对和自动化业务各需相应验收。

## 自动化编译基础

构建从本地SDK依赖读取固定清单和许可，核对 `sdk-runtime.json` 后将运行资产放入发行包；运行时不读取邻仓。`automation-expression.mjs` 使用SDK当前程序模型，提供有界表达式编译、不可变输入、完整引用提取和三值求值。整数串比较保持精度；缺字段、null和来源不可用分别处理，步骤结果还要求成功及合格完成证据。

`compileProgram` 接受 definition_id、definition_version、policy_ref、program、input_bindings、action_bindings、alias_table 七项固定输入，返回规范CompiledProgram、完整依赖图及摘要。规则和状态转换分别形成执行分支，检查结果Schema、参数槽、依赖方向、别名摘要及资源交叠，后继从完整图计算。literal与parameters中的业务同名字段保留原义。此内部模块不提供HTTP发布入口，输出 `approved=false`、`runtime_ready=false`；真实绑定批准、发布、RunGrant和执行仍待接入。

自动化模块测试为 `test/automation-expression.test.mjs` 与 `test/automation-compiler.test.mjs`。安装包独立校验可运行 `node --disallow-code-generation-from-strings test/installed-automation.mjs <已安装包绝对目录>`，会核对包内全部产物摘要，再从安装目录实际加载编译器。

## 参数模板与完整Schema校验

编译结果的 `parameterExecution({snapshots, schemas})` 创建本次执行的内存参数求值器。`snapshots` 按输入binding_id提供可信来源快照，非步骤结果在创建时复制固定；`schemas` 按动作binding_id提供 `{schema_ref, document}`，须与完整动作绑定中的参数Schema引用相同，digest须等于Schema文档JCS的SHA256。快照和Schema必须由可信来源适配器取得，此接口不接受HTTP请求，不通过内容摘要授予权威资格，也不推导任意Schema的可读projection或可写slot。

调用 `resolve(step_id, {results})` 时固定所需步骤结果，只有succeeded且evidence_qualified的结果可用。每步第一次求值的Promise被保留，成功、unknown和校验失败均不能通过更换结果重试；修改调用方Map或对象不会改写已固定快照。此锁存仅在本进程内有效，不替代持久执行账本，也不能跨重启恢复旧执行。

参数只写入已编译批准槽：支持根替换、转义键和已有数组索引，禁止数组追加、前导零索引、原型路径及自动补建中间对象。对象最终成员可由槽赋值创建。缺字段、来源不可用和类型错误返回unknown；null作为明确值进入Schema校验。最终仍须为对象，并校验完整参数的required、枚举、数值边界、条件及组合约束，包括没有动态绑定的静态字段；不自动补默认值、类型转换或移除字段。

核心继续禁用字符串代码生成。完整Schema由独立短生命周期校验子进程处理，四份校验组件从Controller构建清单按 `validation-runtime.json` 固定摘要与许可采用；Ajv及格式库使用相同精确版本。单Schema和最终参数各最多1MiB；执行的快照／Schema复制总量及每步模板复制工作量各最多16MiB。子进程沿用3秒期限、并发2及V8堆预算，堆预算不等于完整操作系统内存隔离。外部Schema引用、非法Schema及子进程失败均拒绝，不进入动态求值回退。

`status: validated` 只证明本次完整参数校验通过，并返回固定参数摘要与Schema引用；它不证明来源仍当前、绑定已批准或可以发送设备命令。权威来源、发布与执行账本、每次发送前重新核验仍待接通。`test/automation-parameters.test.mjs` 运行真实校验子进程；测试Schema和来源快照为合成材料。设置测试专用 `LEAFSEAMER_PARAMETER_PACKAGE_UNDER_TEST` 为已安装包绝对路径，可验证独立产物的相同路径。

## 定义草稿持久化

`openStorage` 的 `drafts` 提供 create、update、updateMetadata、get 内部服务。内容和元数据分别CAS修订，保存不可变历史；定义、修订、请求原结果与通知在同一writer事务提交。幂等身份包含服务／库／请求epoch／主体／操作／键，业务摘要保留嵌套context字段；换context但同epoch、同主体、同键返回原结果，不重复编辑。读取原结果之前及提交之前重核当前权限。当前开发容量为4096个定义、16384次已提交写请求，不回收去重历史。

业务模型直接采用SDK的Lifecycle草稿模型。`draftAuthority.authorizeInTransaction` 只能由可信本机装配提供，必须同步验证当前身份、context、对象权限和认证时钟；范围、epoch、操作、目标和授权摘要在writer内核对。普通CLI、环境和HTTP不接受该提供者，未装配时草稿读写均拒绝。已激活配对的人员身份接收和本地授权交集见下节；服务身份接收仍未接通，公共草稿HTTPS路由见下节，存储测试使用合成授权提供者，不证明真实鉴权。

草稿表属于当前尚未发布的初始数据库布局。布局摘要不匹配的旧开发库继续拒绝打开；本次没有迁移、覆盖或清空已有数据库。`test/automation-draft-store.test.mjs` 使用独立文件库验证重启、CAS竞争、原结果重试、撤权、逐写回滚、历史不可变及请求／通知不可拆提交。

## 内部配对组件

配对状态、挑战、证明和撤销使用与Controller相同的运行组件。构建依赖本地Controller已构建包，按 `pairing-runtime.json` 核对清单、选定组件、认证时钟值模块与许可，然后复制必要资产；安装包运行不加载Controller邻仓。密码KDF不随配对组件导入Seamer。

可通过 `npm run build -- --controller-package <绝对目录>` 显式采用另一份已锁定的 Controller 包。指定目录包含 `dist/build-manifest.json`、相应资产和 `LICENSE`；构建同时核对完整清单、逐文件摘要和许可。跨主机配对需要采用含网络监听功能的组件。可与 `--native-artifacts` 同时使用，由各自的锁文件独立核对。

`openStorage().bindPeerPairings({clock, issuer, management})` 是可信本机装配接口。issuer固定指定Controller的服务／签发者／epoch；management必须同步核验当前远端人员身份、定向audience、近期认证和auth.manage授权。缺来源、服务主体、签发者不符或事务末尾人员／会话变化均拒绝并回滚。Seamer只保存已核验人员的外部身份引用，没有identity_issuer表，不自行签发人员身份。

独立TLS监听在pending阶段只开放status、challenges、proofs，不能调用普通introspection或设备接口；activated后可按明确用途调用Controller核验端点；两端须分别stage、证明和批准，单端激活不代表对方已激活。新实例从本节的本机导入流程开始。首次授权的签发和 Controller 批准通过 Controller 本机人员会话完成。

`startSeamer` 可由可信本机代码通过 `pairing` 参数装配独立TLS监听，固定本机持久身份和指定签发者，不接受HTTP或环境变量传入管理授权。启动失败关闭已启动的HTTP及TLS、停止认证采样并释放数据库；正常 `close()` 统一关闭HTTP、TLS、认证时钟和文件库，重复关闭返回同一完成结果。

`test/controller-pairing.test.mjs` 在同一测试宿主中运行Controller与Seamer组件、独立文件库和实际双向TLS，验证拒绝服务主体、来源失效、事务末尾换人、双向证明、分别激活和撤销。Controller使用真实本地人员会话，Seamer人员核验和环境资格为合成装配；这不替代真实身份接收或首次配对部署验收。

`test/pairing-process.test.mjs` 另以两个独立Node进程运行Controller与Seamer，覆盖真实双向TLS证明、分别激活、Controller真实人员令牌签发、Seamer核验接收及配对撤销后句柄失效；首次stage与activate的Seamer管理核验仍为合成装配。关闭后在子进程仍存活时检查HTTP与TLS端口均拒绝TCP连接，再退出进程。同一套测试也覆盖正式Controller启动器的HTTPS令牌签发、内部核验、注销自动交付及统一关闭；测试专用 `LEAFSEAMER_CONTROLLER_PACKAGE_UNDER_TEST` 可选择其安装包。设置测试专用 `LEAFSEAMER_PACKAGE_UNDER_TEST` 为安装包绝对目录，可对安装包重复同一路径；它仅由测试夹具读取，不是产品配置。同一进程链还验证本地未批准编辑时拒绝、明确批准后创建／读取草稿、幂等返回原件以及减权后拒绝读取和查原。该用例以预置首位管理员开始。首次引导由 test/first-pairing-import.test.mjs 在独立进程中验证，跨主机验收仍需完成。

`startSeamer` 默认将配对管理接到本地授权库的真实身份适配器：管理输入是接收器取得的不透明人员句柄，必须同时具备定向令牌和Seamer本地auth.manage，并处于原近期认证区间。指定issuer、对象身份及本地批准均在同一writer内核对。测试或受信部署装配若显式提供了外部management，可调用 `pairing.useIdentityManagement()` 单向切换到该真实适配器；此接口不创建人员、配对或管理员批准，切换后原来源不能通过它恢复。

撤销当前身份核验配对时，只允许该次revoke事务在末尾核对精确撤销前后原件。内部临时句柄不可由请求构造、复制或跨事务使用，回滚与提交均销毁；原认证时间、近期认证、配置和本地批准仍须有效。它不把已撤销配对重新标为active，不延长身份快照，也不允许撤销完成后的新管理调用。其它配对仍按各自当前状态裁决。

常规管理的双进程回归从已激活配对开始，检查旧凭据和仅有 `auth.read` 的令牌均被拒绝，并验证新配对登记、双向证明、人员批准和当前身份配对的撤销。新实例使用下面的本机首次配对流程。

### 本机导入首次配对授权

在受保护的 Seamer 主机停机操作。仅保存授权需要 Schema 18，完成配对至少需要 Schema 19。按当前安装版本的要求显式升级数据库，已有更高修订的库直接导入。将 Controller `pairing-admin` 的 `export` 输出逐字节保存为授权文件。两份公共证书来自管理员已独立核对的 Controller 和 Seamer 配置，可使用 PEM 或 DER。

```text
leafseamer migrate-storage --data-dir <数据库绝对目录> --to-revision <19或更高的目标修订>
leafseamer import-first-pairing --data-dir <数据库绝对目录> --authorization-file <授权文件绝对路径> --controller-certificate <Controller 证书绝对路径> --seamer-certificate <Seamer 证书绝对路径>
```

导入验证签名、两端证书绑定及目标服务和数据库身份。授权及证书原件同事务保存，原样重投返回相同收据，授权 ID、配对尝试或本地绑定上的冲突被拒绝。已有配对激活记录或本地权限的数据库只允许查回此前已导入的原件。`state: imported` 表示原件已保存，数据库仍处于保护态。

Schema 19 在 `startSeamer` 的本机受信装配中提供 `pairing.stageFirstPairing(authorizationId)` 和 `pairing.completeFirstPairing(authorizationId)`。先配置原双方证书和端点，使用合格认证时钟登记导入授权对应的配对，再通过现有双向 TLS 完成证明。Controller 独立批准激活后，Seamer 对照原两端证明，将本地激活、授权消费及首位管理员批准在同一事务保存。首位批准限 `auth.manage` 和 `auth.read`，到期时间取原配对期限。之后人员还需 Controller 针对 Seamer 签发的当前定向令牌，管理继续经正常人员核验。重复完成只返回历史收据。

### CLI 受信配对启动

```text
leafseamer start --data-dir <数据库绝对目录> --pairing-profile <受保护配置的绝对路径>
```

本机受保护的 JSON 配置使用 `format` 值 `leafseamer.seamer-pairing-profile/1`。证书、私钥、公钥和证据文件都使用绝对路径。

| 字段 | 内容 |
| --- | --- |
| `scope` | 本库 `service_id`、`database_instance_id` |
| `local` | `credential_id`、`host`、固定 `port`、`certificate_file`、`private_key_file`、含 1 到 8 个 PEM CA 路径的 `ca_files`。默认回环监听，可选 `network` 和 `server_name` |
| `controller` | `identity` 中的对端 `service_id`、`database_instance_id`、`service_kind`、`issuer_id`、`issuer_epoch`、`credential_id`，以及原配对的 HTTPS `origin`、`server_name`、`certificate_file`、`trust_bundle_digest` |
| `clock` | 经部署批准的 `policy_digest`、由外部提供者写入的 `evidence_file` 和已固定的 Ed25519 `public_key_file` |
| `first_pairing_authorization_id` | 已导入的一次性授权 ID。仅启用已配置配对时设为 `null` |
| `public_api` | 可选。提供固定回环 `host` 和 `port`、`certificate_file`、`private_key_file`、本端配对 `binding_id`、精确 HTTPS `allowed_origins` 和布尔值 `local_permissions`。可选布尔值 `local_event_bindings` 默认关闭，启用要求 Schema 11 或更高版本 |
| `publication_policy` | 可选。从 `approval_file` 的绝对本机路径读取部署批准，并用 `approval_sha256` 固定文件原字节 |

Seamer 的时钟包络可由 Controller 安装包中的 `dist/clock-evidence.mjs` 在 Seamer 宿主签发。部署方为当前 Seamer 实例固定独立签发配置，向工具交付 Seamer 实际启动挑战及匹配的政策、运行域和健康原件。签发成功后服务仍按固定 Ed25519 公钥和当前启动重新验证。

直接跨主机配对使用 `local.network: true` 和本机固定单播 IP、端口。`local.server_name` 可固定证书中的精确 SAN。双方按各自对端的实际 `origin`、证书和信任包完成双向证明。草稿、事件审批和本机权限管理继续使用独立的回环 `public_api`。

进程先输出 `authentication_clock_challenge`，携带本次启动 ID、数据库身份、原生时钟制品摘要、连续性标识和单调刻度。受信时钟证据提供者据此提交当前启动的 `policy`、`runtime`、`health` 原件，并以原子替换更新证据文件。文件使用规范 JSON 包络 `{format, evidence, signature}`，`format` 为 `leafseamer.authentication-clock-evidence/1`，`signature` 为 Ed25519 对格式标识、一个 NUL 字节和规范 `evidence` 的签名，编码为无填充 base64url。启动核对本机身份、签名和原策略后才监听。当前安装包提供接收和复核入口，部署方提供经批准的时钟证据源。

选择首次授权时，CLI 输出 `first_pairing_staged` 并通过现有 TLS 通道交换证明。Controller 管理员通过本机 `pairing-admin` 完成本端证明与激活。Seamer 确认后输出 `first_pairing_completed`。按相同授权重启且取得新启动的合格证据后，只返回旧完成收据。

## 已激活配对的身份接收

可信启动器提供 `pairing.identityReceiver.prepare({binding_id,token,required_revision})` 和 `current(handle)`。prepare经固定Controller的双向TLS与当前SDK核验身份，返回进程内不透明句柄；current使用Seamer自己的认证时钟重新检查原配对、运行域、UTC区间和原挑战单调截止。传输耗时包含在最多两秒的新鲜窗口中，响应不能重新开始计时；失效句柄被删除，恢复时钟或配对也不复活。关闭服务清除全部句柄，身份原件不可变，不保留令牌秘密。

该句柄仅证明短时身份，近期认证仍保留原会话区间。可信启动器将它接入下述本地对象授权、草稿服务及显式启用的HTTPS路由；签发方权限不能直接成为目标管理员。`test/peer-identity.test.mjs`覆盖原挑战截止、错挑战／签发者／目标／水位／近期认证、句柄不可伪造以及换域和关闭失效。

接收器必须装配本服务SQLite的 `identityWatermarks`。首次交付身份前，将通过当前配对、域、UTC和原挑战期限核验的签发水位可靠提交；观察按签发服务／issuer／epoch隔离，记录原快照摘要、签发boot、配对引用及本地boot，不保存令牌秘密。相同水位不重复写入，推进历史不可修改或删除；当前开发容量16384条，满时拒绝新水位，不清理历史来放行。属于尚未发布的初始库布局扩展，不自动迁移、覆盖或清空旧开发库。

后续introspect的required_revision取调用方要求与已持久水位的较大者。响应返回时重新检查水位，防止较旧的在途回复覆盖新观察；已发句柄每次使用也检查当前水位。全局水位推进会使较旧句柄失效，须按当前权限重新核验，不能靠旧两秒窗口继续使用。写入失败关闭该接收器并清除句柄，不能返回尚未持久确认的身份成功。重启保留水位，不恢复旧句柄，也不延长原token、context或已受理请求。

这只是已核验身份的防回退基线，不是完整撤销传播。断线期间尚未收到的签发方变化仍不能凭空获知；会话拒绝交付见下一节，其余对象拒绝、运行保护与完整回执仍须接通，不能把水位持久化显示为撤销已完成。双进程用例验证真实mTLS核验、旧句柄因新水位失效，以及关闭后由另一个Node进程重开文件库读回原水位；首次配对及环境资格继续使用测试预置。

## 人员会话撤销交付

已激活配对可明确授予签发Controller `auth.revoke` 用途，通过独立内部双向TLS的 `POST /internal/pairings/{binding_id}/session-denials` 交付 `leafseamer.session-denial/1`。pending配对、缺用途、错证书、错签发服务／issuer／epoch或目标数据库均拒绝。此入口不接受浏览器Cookie，不用已撤销人员重新认证，也不向普通健康HTTP开放。

消息包含签发服务与库、issuer／epoch、原撤销operation引用、session_id、revision_floor及目标服务／库。结构精确、正文必须为规范JCS，最多16KiB；未知字段、重复键、分块传输均拒绝。当前只处理人员会话终态，不能用于主体、服务凭据、grant或信任撤销。已撤销session_id不复用，接收记录永久阻止该会话的旧句柄及后续快照，不因更高session_revision复活。

拒绝记录与 `leafseamer.session-denial-receipt/1` 原回执在同一SQLite writer保存，提交末尾再次核对当前配对。相同操作与正文返回原件，同操作换内容拒绝；回执绑定完整消息摘要、目标服务／库、接收boot及不可复用receipt_id。写入失败或响应丢失不被视作未接收，发送者只重取同一通知的原回执。当前开发容量16384条，不自动清除历史。

回执的 `identity_state=denied` 仅表示身份使用已被拒绝；`protection_state=pending` 明确保留运行／设备保护未确认。Controller可以可靠保存此回执，但原撤销目标与全局传播仍保持pending，不能显示为applied。Controller内部配对通道已支持显式装配的自动会话拒绝调度，从持久outbox恢复并校验已存回执后跳过重复发送；其余撤销类型、运行保护及完整目标确认尚未实现。

接收器在当前身份和业务writer核验时查询这些持久拒绝记录。测试覆盖同事务失败／静默忽略回滚、错范围与重复消息，以及真实Controller会话注销、双向TLS投递、接收后发送端尚未登记回执、随后补记相同回执和新进程重开后继续拒绝。跨主机和生产部署仍待验收。

## 本地批准与草稿授权

`startSeamer` 在配置配对时装配 `authorization` 和 `drafts` 内部服务。授权库以指定issuer／epoch／principal三元组建立独立、不可复用的subject映射，保存不可变批准修订、CAS当前指针、原操作及同writer通知。`authorization.replace` 要求真实身份上限与本地auth.manage交集、human及原近期认证区间；事务末尾再核期限。批准明确权限、all或selected定义范围及认证有效区间，不从管理员名称、远端登录或首次查询自动授予权限。缩权、缩时或缩范围使批准进入pending并立即拒绝使用；保护通知尚未交付确认，pending不能直接重新激活。

`createRequestContext` 要求request.create及所请求草稿操作的两侧权限交集。当前内部实现使用最多60秒、256次新写入的有界context，固定当前SDK选择、boot、请求epoch、主体／会话／凭据、令牌绑定摘要、授权修订和运行域。相同人员的另一令牌不能搬用context，重启后旧context失效；每次消费仍须新鲜身份原件，context期限不延长身份快照。令牌绑定摘要只用于内部关联，不是可兑换凭据。

草稿读写在自己的SQLite writer内同步复核已激活配对、短时身份、本地批准、定义范围及context。新写额度、定义内容／metadata、原请求结果与通知一并提交；失败一并回滚，已提交幂等重试不重复扣费，返回历史原件前仍核当前读取权。`currentInTransaction` 使用现有配对事务核验入口，避免在业务writer内另开事务。

本机首次授权通过 import-first-pairing 停机导入。后续本地权限管理、context 和草稿通过受信装配及显式启用的业务 HTTPS 入口访问。`test/local-authorization-fixture.mjs`的SQL预置仅用于独立测试，不随包发布。服务令牌实际接收、撤销通知确认及完整运行保护仍待交付。

## 公共草稿HTTPS

可信装配可为 `startSeamer` 提供 `publicApi: {cert, key, binding_id, host, port, allowedOrigins}`，CLI 通过配对配置的 `public_api` 启用同一 HTTPS 入口，并在 `listening` 事件返回 `public_origin`。首次引导时 `binding_id` 须匹配授权中的 Seamer 绑定。公开 TLS 和内部双向 TLS 分别监听，证书由装配方提供。监听失败统一关闭已启动的端口并释放文件库。

### Linux 保护态 HTTPS 启动

先初始化并显式迁移 Seamer 数据库，再运行安装包中的入口。

`node --disallow-code-generation-from-strings dist/cli.mjs start-protected-ingress --data-dir /srv/leafseamer/seamer --ingress-profile /etc/leafseamer/seamer-ingress.json`

本机受保护的 JSON 配置采用 `format` 值 `leafseamer.seamer-protected-ingress/1`，包含 `service_id`、`database_instance_id`、`address`、`port`、`origin`、`certificate_file` 和 `private_key_file`。IP 必须匹配当前网络命名空间内分配的接口地址，端口固定。证书和私钥使用绝对路径，由部署者保护。证书覆盖 Nginx 校验的上游名称，`origin` 使用实际管理 HTTPS 源。

启动记录写入前校验服务和数据库身份。公开监听复用现有 API 的 TLS、Host 与 Origin 检查，缺少凭据返回 401，带格式有效的凭据返回 503。健康口继续绑定回环。实际配对和授权操作经 `--pairing-profile` 对应的受信启动路径启用。

### Linux DF02 闭锁准入

可信 Suite 装配通过 `contained-core-linux --data-dir <绝对数据目录> --bootstrap-policy <规范 JSON>` 启动 Seamer，回环地址和端口可用 `--host`、`--port` 指定。进程通过定向继承的 FD 3 使用既有 SDK 引导，核对 Node 和 Seamer 构建清单的摘要、数据库、cgroup 及本机单调域。最低 Schema 20 的业务库要先保存闭锁回执，随后才开启保护态回环诊断。监督通道断开时关闭服务。授权操作、服务配对与生产授时仍需完整部署装配。

Suite 的 Compose 计划可使用 `contained-compose-linux --data-dir <绝对数据目录> --ingress-profile <绝对路径> --bootstrap-policy <规范 JSON>`。核心从 FD 3 完成闭锁准入并保存回执。可附加 `--clock-evidence-profile <绝对路径>` 采用受保护的 `/2` 配置，容器内回环的 `/health/ready` 可查询采集状态。启用 `--pairing-profile` 时，时钟取得资格后启动绑定到指定配对的业务 HTTPS，本机权限管理在独立容器回环监听。Compose 首次授权由同一运行代内的 Seamer 从宿主只读挂载目录采用。[Suite](https://github.com/LeafSeamer/leafseamer-suite)

已配对的 Compose Seamer 可从 `publication_policy` 采用安装批准。该批准由 Suite 固定摘要并只读挂载。Seamer 按 Linux 受保护文件规则读取，核对原件、服务库、策略和有效期，再由现有发布事务检查人员授权与当前绑定批准。[Suite 部署说明](https://github.com/LeafSeamer/leafseamer-suite)

可用 `--clock-evidence-profile <绝对路径>` 检查本次 Seamer 启动的签名授时证据。配置 `format` 为 `leafseamer.seamer-linux-clock-evidence-profile/1`，其余字段为 `service_id`、`database_instance_id`、`policy_digest`、`public_key_file` 和 `exchange_directory`。服务与库标识须匹配本次引导策略。公钥和交换目录使用绝对路径，由 root 或服务账户持有并防止其他账户写入。Seamer 使用自己的交换目录、策略和数据库身份。

保存闭锁回执后，Seamer 发布 `challenge-<service_boot_id>.json`。签发方在同一原生时钟域用该挑战签发 `evidence-<service_boot_id>.json`，并让服务账户能够读取证据。回环 `GET /health/ready` 的 `authentication_clock_evidence` 报告 `missing`、`verified`、`expired` 或 `rejected`。`verified` 表示本次签名、作用范围和原期限通过核验，服务仍处于 503 保护态。

使用 `leafseamer.seamer-linux-clock-evidence-profile/2` 可让 Seamer 为本次启动持久保存认证采集。字段和签名格式沿用 `/1`。它要求真实 sysfs 中不存在 `/sys/power/state`，并在采集前后复核挂载视图。回环就绪响应继续返回 503，并用 `authentication_clock` 报告 `unqualified`、`available` 或 `faulted`。该配置只覆盖 Linux 内核的系统睡眠条件，虚拟机外部恢复仍由部署环境处理。

及时续签时保持 `policy` 和 `runtime` 原件，按现有签发流程生成新的 `health` 原件。它须在旧原件截止前到达，且由一次新采集事务采用。替换证据前先设置服务账户的读取权限，再在同一文件系统内原子替换目标文件。旧采集保留原截止，证据失效或撤下后当前运行域持久锁闭。

受监督 Linux 启动可同时提供 `--pairing-profile <绝对路径>` 和 `/2` 的 `--clock-evidence-profile`。Linux 配对格式为 `leafseamer.seamer-pairing-profile/2`，沿用 `scope`、`local`、`controller`、`first_pairing_authorization_id` 及可选公开 API 和发布策略字段。`scope` 与引导策略必须绑定同一服务和数据库；时钟由单独的证据配置提供。TLS 文件和配置使用受保护的绝对路径。

先用 `import-first-pairing` 将 Controller 人员签发的一次性授权导入目标 Seamer。受监督启动取得首次持久认证采集后才开启配对和公开 TLS。配置中的授权 ID 用于首次完成和重投查原；监督通道关闭时停止配对和公开监听。

启用 `public_api.local_permissions` 后，可在本机以 `POST /auth/local-permissions/v1` 管理本地权限。使用发给该 Seamer 的人员 Bearer 令牌和 `Content-Type: application/json`。此路由接受本机直连 HTTPS，请求不携带 Cookie、Origin 或代理头。正文使用规范 JSON `{operation, request}`。读当前授权时用 `inspect` 和 `request: {principal_id}`。写授权时用 `replace`，提供 `request_id`、`principal_id`、`principal_kind`、`expected_revision`、`permissions`、`definition_scope`、`not_before_ns` 和 `expires_ns`。写入要求已有本地管理权限、当前签发方权限和近期认证。

首次配对只登记 `auth.read` 和 `auth.manage`。创建草稿前，由 Controller 为该人员配置定向权限上限，再由 Seamer 管理员显式批准 `request.create` 和 `definition.edit` 等业务权限。读取草稿需要 `definition.read`。

草稿编辑的五项SDK操作为 `createRequestContext`、`createDefinition`、`getDraft`、`updateDraft` 和 `updateDefinitionMetadata`。每次请求使用定向Bearer，经已激活配对重新取得短时身份；不接受Cookie身份或请求正文指定签发者。读取遵循现行getDraft契约，无需另造context头，仍核当前定义读取权；三项写入使用正文context及既有CAS／幂等规则。草稿响应沿Lifecycle正文，context沿具名标准信封，不混用两种返回格式。

入口要求TLS 1.3、当前SDK选择和精确Host。出现Origin时必须为自身或显式登记的HTTPS origin；预检仅允许对应方法及明确的认证／选择头，不接受通配origin或Cookie凭据。许可origin的响应显式公开X-LeafSeamer-Contract与X-LeafSeamer-Contract-Digest，供浏览器中的SDK核对当前选择。正文最多16MiB、同时8项请求及总声明容量32MiB，拒绝分块传输和超出声明长度的正文；正文接收限时5秒。公开错误不回显秘密、原始请求或异常全文，响应释放前再次核验当前权限。写入提交后若响应阶段失效，错误不证明没有提交；客户端必须按原幂等身份及当前权限查原，不换键自动重做。

`test/pairing-process.test.mjs`经两个实际进程和SDK验证HTTPS的创建、读取、内容／元数据修改、幂等、CAS冲突、缺凭据、错误Origin、畸形／超大报文以及撤权后的读写拒绝；关闭后检查全部监听端口。`test/draft-http.test.mjs`以合成授权提供者专门验证响应释放边界。同一测试通过SDK浏览器会话客户端实际登录Controller并签发Seamer定向令牌，使用独立草稿客户端完成五操作；跨域响应按浏览器可见头规则过滤，Cookie容器为测试模拟。原HTTP预检与模拟Cookie容器不能替代浏览器；新增实际Edge用例现已通过登录、创建、内容／元数据编辑和响应丢失后的重载查原，首次引导及生产PKI仍待完成。

`lookupAutomationRequest` 现以只读POST接入草稿原请求。原context只用于定位原请求epoch与本人，不要求仍处于旧写入期限、boot或令牌绑定中；查询仍须当前指定签发者的本人身份、request.read及原定义当前读取权。返回保存的DraftRecord或PublishedVersion与原请求摘要，不创建新context、不扣写额度、不重新受理；未知记录或失去权限不授予重发资格。发送前再核当前读取权。首次写入、同键重试及其它自动化操作的期限和准入不因此放宽。

实际浏览器验收同时发现并修复GET预检错误：只对有正文的方法提供Content-Type做路由校验，GET／HEAD预检按无正文规则处理。相关HTTPS回归覆盖GET，跨域成功响应仍公开当前SDK选择头。GUI当前提供内容文档编辑，不代表完整图形编排、绑定批准或运行功能已完成。

## 定义发布与历史读取

新库仍显式初始化为 schema 1；启用发布存储前停止服务，执行以下独立迁移。普通打开不自动升级，不替换服务身份或请求 epoch。迁移保留旧草稿、请求、配对与身份表，核对旧迁移摘要及全库外键；任一失败整笔回滚，已迁移数据库重复执行只返回当前身份。

```text
node --disallow-code-generation-from-strings dist/cli.mjs migrate-storage --data-dir <数据绝对目录> --to-revision 2
```

可信装配可向 `startSeamer` 传入 `publicationSource`，内部 `publications.publish(authentication, definitionId, request)` 与公开 `publishDefinition` 共用同一存储事务。发布要求实际人员身份、两侧 definition.publish 交集、当前定义范围与原 context，服务主体不能取得本地发布批准。请求只固定草稿修订、内容与选择摘要。版本号独立预留且不重用；编译与完整静态参数 Schema 验证在写事务外执行，提交前后同步复核原授权和当前来源，草稿内容变化拒绝提交，元数据修改不改变固定内容。

`publicationSource.resolve({scope, definition_id, draft})` 必须从权威注册解析精确选择，返回 `{bindings, alias_table, schemas, review_ref, review}`。每项 bindings 为 `{kind, binding_id, content_ref, value}`；schemas 为 `{binding_id, schema_ref, document}`。review 固定 scope、selection_digest、alias_table_digest、policy_ref、按 schemas 顺序排列的 parameter_schema_refs，以及 `parameter_scope: 'all_allowed_parameters'`；review_ref 摘要绑定该原件。该结构校验只验证内容关联，不能代替来源批准。同步 `assertCurrentInTransaction({scope, definition_id, source_draft_revision, content_digest, selection_digest, snapshot})` 必须实际复核注册、投影、Alias、策略、审阅及所有允许参数的完整作用范围资格，成功返回该固定 snapshot 的 JCS SHA-256 摘要；异步返回或任何变化均拒绝。不得把客户端自报摘要、模板 complete 或实验结果当批准。普通 CLI 没有默认来源，缺来源返回503，仍不具备生产发布资格。

Schema 11 的可信装配可使用 `publicationPolicy.assertCurrentInTransaction(policyRef)`，以同步返回 `true` 授权使用精确策略。此入口从两类本地批准账本解析草稿所选绑定，以获批的别名和参数 Schema 编译，在静态验证完成后重新读取当前配对来源。发布事务的首尾均复核策略、批准修订和来源，制品审阅摘要绑定实际使用的批准引用。撤销阻止使用该批准创建新版本，历史查回仍使用原制品。单次装配只接受一种发布来源。

CLI 通过 `publication_policy` 采用上述发布入口。安装方在受保护目录放置规范 JSON 批准文件，包含 `format: "leafseamer.publication-policy-approval/1"`、本库的完整 `policy_ref` 以及十进制 UTC 纳秒窗口 `not_before_ns` 和 `expires_ns`。配置中的 `approval_sha256` 格式为 `sha256:<小写十六进制>`。由部署方选定并固定的审批文件提供授权依据。发布请求须使用相同策略引用，并取得人员发布权限及当前绑定批准。

为实时事件启用条件求值时，在 Schema 24 使用 `leafseamer.publication-policy-approval/2`。在原批准字段之外提供 `event_age_limits` 和 `max_condition_nodes`。每条限额包含发布者的 `authority_id`、含 `id`、`revision` 和 `digest` 的精确 `registration_ref`、`capture_semantics` 及非负整数 `max_age_ms`。每份来源注册只出现一次。`max_condition_nodes` 限制一次摄取为所有匹配运行展开的条件工作量。将 `policy_ref.digest` 固定为 `{event_age_limits,max_condition_nodes}` 规范 JSON 的 SHA-256，并在 `approval_sha256` 固定整份文件的原字节摘要。更新限额时重新批准、固定并发布相应策略。未配置某个来源限额的事件保持保护。

离散来源使用 `event_occurrence`，采样来源使用 `sampled_observation`。事件的保守年龄上界可等于 `max_age_ms`。Seamer 核验已布防运行、当前批准和来源基线，然后用精确匹配的事件值求值。当前输入不足或最高优先级并列时，Seamer 原子保存决定并暂停运行。

启用触发计划及提交后锚定前，停机执行 `leafseamer migrate-storage --data-dir <数据库绝对目录> --to-revision 26`。条件为真且所选动作的当前 RunGrant 仍有效时，Seamer 原子保存触发决定、完整计划和初始等待步骤。计划固定参数模板及依赖。

原事务提交返回后，本次接收路径才采集同域执行锚，并在短事务内复核运行批准、配对、发布依赖及 RunGrant，把锚和无前驱步骤的首次就绪原件一起保存。就绪仅供本机运行器使用，设备写入仍需实际参数验证及 Controller 出口准入。锚或就绪写入失败时，原接收结果仍可查回，活动计划保持暂停。重投、重启及显式迁移均保留历史计划，只有本次成功提交的接收路径能够建立执行锚。

本机延迟推进需要停机执行 `leafseamer migrate-storage --data-dir <数据库绝对目录> --to-revision 27`。延迟沿用当前编排的 `execution_admitted` 锚点和原 `delay_ms`、`max_lateness_ms`，在原单调域的 `[到期, 截止)` 窗口内记录到期。本机调度器在写入前复核运行批准、配对、已发布依赖与来源代次。直接后继为另一段延迟时，首次就绪消耗相应前驱窗口。直接后继为动作时，就绪原件保留所有直接前驱窗口，动作执行仍需 Controller 准入。逾期、运行资格失效或存储失败进入保护，重启后保留原件并暂停活动计划。

Schema 28 在动作就绪后从已接收的原触发事件取值，验证已发布的完整参数 Schema 和原 RunGrant 的参数约束。SQLite 写入前再次核对当前配对、授权、同域时钟及直接延迟窗口。封存的参数原件绑定执行、步骤、事件摘要、Schema 和 RunGrant，重投与重启沿用该原件。守卫未得到真值、参数不可用、授权不符或窗口到期时，运行进入保护。动作写入设备仍需 Controller 完成命令准入和发送门控。

停机后执行 `leafseamer migrate-storage --data-dir <绝对路径> --to-revision 29`，并采用支持 `leafseamer-automation-command/1` 的配对运行时和 Controller Schema 95。实时动作在分开的 SQLite 事务写入原请求和唯一尝试标记。收到尝试事务提交结果的当前执行路径可调用一次提交接口，调用前再次复核原授权与截止。配对服务只查询已有尝试的原请求，查无记录保持 `indeterminate` 并保护运行，之后继续查回原裁决。`runs.getExecution` 显示原尝试与裁决，受信本机调用方可凭当前运行读取权调用 `runs.reconcileAutomationSubmission`。Controller 排队受理后仍需自动模式设备门控、实际发送及结果证据，执行步骤在此阶段保持待处理。

配对终态读取需要支持结果接口的 Controller Schema 97 安装包。Seamer 自 Schema 30 起按原配对、原提交身份和原裁决查询终态。`runs.getExecution` 在原提交下提供其持久终态。未知终态与运行保护同事务提交，响应丢失或写入失败可按原命令继续查回。新提交在发送前为终态预留存储，既有提交保留原预算。

停机执行 `leafseamer migrate-storage --data-dir <数据库绝对目录> --to-revision 32` 可启用普通触发的最终提交。运行器核对原执行和制品、布防代次、配对及目标资格、来源和原截止，再把完成原件、审计、固定事件 ID 的通知、运行修订和活动执行释放写入同一事务。状态机同时提交目标业务状态，规则的业务状态保持为空。成功运行回到 `armed`，下一事件重新核对证据并可受理。设备动作完成后若最终提交失败，`runs.getExecution` 返回对账所需的原件并标记 `reconciliation_required`。重启后仍可按当前读取权限查回原完成记录。

Schema 33 增加执行完成通知的本机交付回执。停机显式迁移后，受信嵌入程序可通过 `startSeamer({ executionNotificationSink })` 接通消费端。`executionNotificationSink` 提供安装时固定的 UUID `consumerId` 和 `deliver({ event, ack }, signal)`。消费端应按 `event.event_id` 幂等保存原通知，提交自己的事务后原样返回 `ack`。Seamer 核对服务与数据库、消费者、事件 ID 及载荷摘要，随后持久写入回执。回执落库前中断会重送同一事件，已确认的事件可在重启时跳过。Seamer 通过 `signal` 通知消费端停止本次交付，并通过 `executionNotificationStatus()` 提供交付计数。公共 C05 订阅和浏览器消费仍待接通。

已启用业务 HTTPS 的安装可用当前 Product9 SDK 读取发布版本和激活槽。`getDefinitionVersion` 按定义 ID 与版本读取原发布结果，要求 `definition.read`。`getActivationSlot` 按定义 ID 读取槽修订、占用运行和定义版本，要求 `run.read`。未占用的槽返回修订 `0` 和两个空引用。响应释放前复核对象授权与原投影，R5 完成后仍可查到已布防运行所占的槽。

启动时核对审批摘要和服务、数据库身份。发布和依赖该制品的资格检查会重新读取固定文件，要求当前认证时间区间完整落在原批准窗口内。运行期间一旦发现文件撤下、内容变化或到期，本次启动内的资格锁存为失效。历史制品与已提交请求仍按原权威及读取权限查回。

编译程序、图、所需独立性证明、发布者授权原件、审阅原件、artifact、PublishedVersion、原请求结果和审计通知在文件 SQLite 的同一 writer 内保存。读取重新核对原件、图与摘要；`getPublishedArtifact`、`getCompiledProgram`、`getIndependenceProof` 及 `lookupAutomationRequest` 仅要求当前读取权，不重获旧写入期限或运行资格。内部读取分别为 `getArtifact(handle,id,version)`、`getProgram(handle,id,version)`、`getProof(handle,id,proofId,revision)`。动态参数最终值仍由既有运行期校验器检查，发布不启动自动化或签发 RunGrant。

公开发布要求已许可 Origin 和 Seamer 独立 CSRF。包含 publishDefinition 的 context 响应通过 `X-CSRF-Token` 返回仅本监听有效的凭据，绑定 context、定向令牌、Origin、服务与 boot；context 创建也必须携带许可 Origin。发布时原样发送该头，换令牌、换 Origin、换 context 或重启监听不能复用。CORS 显式公开该头，不共享 Controller 的 Cookie 或 CSRF。SDK 草稿客户端已提供发布及三项历史读取；响应丢失后使用原请求身份查原，不自动重新发布。

实际文件库与 HTTPS 检查覆盖迁移回滚、逐写失败及静默遗漏、CAS、撤权、来源变化、并发同 key、版本空号、重启历史、提交后响应拒绝和 CSRF 绑定。发布正例的身份与来源为合成测试提供者；当前人员授权与缺来源拒绝另由产品本地权限模块验证。权威绑定注册／批准提供者、真实身份到批准来源的完整发布链、发布 GUI、RunGrant、运行保护和设备验收仍未完成。

旧 NodeCG 实现及历史保留于 [leafseamer-legacy](https://github.com/ikenainanodesu/leafseamer-legacy)。本仓尚未导入旧业务源码；后续复用时逐项记录来源、历史映射和必要许可声明。

本仓主许可为 [Apache-2.0](LICENSE)。


可信服务装配可通过 `application.pairing.transport.readDeviceBindings(bindingId, request)` 从已配对 Controller 读取绑定原件。请求格式为 `leafseamer.device-bindings-request/1`，字段为 `challenge_id`、`scope`、`device_id`、`expected_registration_ref`；调用方为每次读取生成新的 UUID 挑战。`scope` 使用 `leafseamer.device-binding-scope/1`，包含 Controller 服务／数据库身份和排序去重的设备 UUID 列表，其完整摘要必须与已批准的 Seamer 配对条目相同，并具备 `automation.bindings.read` 用途。

共享组件使用实际双向 TLS 核对固定对端，在请求发送和响应接收完成时重核当前配对，验证挑战、范围、注册、绑定、Schema及别名摘要。指定旧注册引用时拒绝，不改用新注册；没有自动重试或缓存回退。双端配对存储和 TLS 已验证用途、范围、来源不可用及撤销拒绝；传输正例的绑定正文使用合成测试来源。本地订阅／用途批准、持续来源当前性与真实模块到发布的完整链路仍待接通，读取响应不能直接充当 `publicationSource` 或 RunGrant。


本地绑定用途批准使用显式 schema3：停止服务后运行 `node --disallow-code-generation-from-strings dist/cli.mjs migrate-storage --data-dir <绝对目录> --to-revision 3`。可从 schema1 或 schema2 升级，原身份、迁移、草稿和发布记录保持不变；升级失败整体回滚，普通打开不自动升级，也不允许降级。schema1／2 的现有功能继续按原版本工作。

schema3 可信启动器提供 `application.bindingApprovals`。先调用 `prepare({binding_id,scope,device_id,expected_registration_ref})` 经具名 TLS 读取注册，使用 `inspect(handle)` 查看完整正文；审批请求必须携带用户实际审阅的 `expected_bundle_digest`，不能把新读取结果自动批准。读取句柄只在原本地单调域内有效，最多两秒且包含网络往返，配对改变或服务关闭即失效；这不是远端来源当前性的租约。交互审阅后应重新读取同一注册，再用原审阅摘要提交。

`approve(authentication,handle,{request_id,approval_id,definition_id,expected_bundle_digest})` 要求真实人员的不透明身份句柄、近期认证、签发方与Seamer本地 `auth.manage` 交集以及定义范围。批准固定到一个已存在定义、完整远端注册／绑定正文、原配对及 `definition_compile` 用途；不会授予事件订阅或运行权限。批准原件、head、请求查原及通知同事务保存；同人同请求只能查回原件，不能换正文或复活撤销结果。`revoke(authentication,{request_id,approval_id,expected_revision:"1"})` 生成不可变第二版并立即拒绝后续本地采用，保护通知仍为 pending，不能冒充运行保护已完成。

`get(authentication,approvalId)` 按当前 `auth.read` 和定义范围读取原件；`check(authentication,approvalRef,definitionId,handle)` 同时核当前本地批准、原配对和新读取的完整正文。业务writer可使用 `currentInTransaction`。注册变化需要新的明确批准，旧引用不会自动换到新来源；读回历史批准不表示当前可用。该检查尚未实现远端持续当前性或发布资格，`publicationSource` 仍需独立提供，普通启动不会自动将本地批准当作完整发布来源。

实际双进程已验证人员身份、TLS注册读取、本地批准、原请求查回、注册变化及撤销拒绝；绑定来源由测试存储装配并使用实际 Controller 注册／解析代码，首次配对及首个本地管理员仍为测试预置。迁移、逐写故障、撤销回滚、重开库及读取句柄原截止另有定向检查；尚无真实模块到发布的全链、批准界面、真设备或生产部署验收。


受监督模块集成已进一步验证：Suite 的 `supervisor_create_module_writable_peer_capabilities` 场景使用实际原生监督器、独立模拟器、Controller 注册及独立 Seamer 安装包。该场景不再预置绑定正文，而是经真实TLS逐字匹配核心生成的注册和绑定；人员批准、断线拒绝、换会话旧注册／旧批准拒绝、显式新批准及运行撤回后的新读取拒绝均通过。测试服务不装配合成 `publicationSource`。此证据关闭模拟器到本地绑定批准的实际集成缺口，批准界面现已接入同版 GUI。持续来源当前性仍需在发布和执行链中验证。


交互式批准业务可使用 `candidate(authentication,definitionId,sourceQuery)` 查看候选正文，以及 `approveCurrent(authentication,sourceQuery,request)` 在确认后重新读取并提交原审阅摘要。`sourceQuery` 显式包含 `binding_id,scope,device_id,expected_registration_ref`；确认时使用候选的精确注册引用。候选读取先核当前 `auth.read` 和定义范围，批准读取先核人员管理权限；网络等待不持有数据库writer，返回后及提交末尾重新核对同一授权，期间权限或来源变化即拒绝。内部短期句柄使用后立即释放，不交给界面保存；这些方法仍可用于可信应用装配；公共HTTP采用下文的product6操作，批准GUI已接入同版SDK，见GUI的绑定批准使用说明。

`lookup(authentication,{request_id,request_digest,definition_id})` 在当前签发方与本地 `auth.read`、`request.read` 交集和定义范围内，按本人身份查询原请求。`request_digest` 是原批准或撤销请求完整JCS的SHA-256摘要。结果包含原请求定位字段和 `result`；未找到时为null，不证明将来不会提交。已提交请求的精确重放与查原不重新访问远端来源；撤销后的批准查原仍返回第一版历史原件，当前状态须另用 `get` 读取。无写权限时不能借幂等重放恢复写入资格。


配对且采用schema3的公共Seamer HTTPS监听器已提供product6五项操作：`POST /api/v1/definitions/{definition_id}/binding-candidates/query`、`POST .../binding-approvals`、`GET .../binding-approvals/{approval_id}`、`POST .../binding-approvals/{approval_id}/revoke`、`POST .../binding-approval-requests/lookup`。精确模型和类型来自同版SDK，旧product5选择拒绝。批准与撤销要求该监听器签发的上下文及CSRF、精确Origin、真实人员近期认证、双端auth.manage和定义范围；请求结果与上下文额度在同一writer中核验保存。额度由不可变批准／撤销原件中的context_id计入，与同上下文草稿写共享总限额，无额外数据库迁移。服务关闭或响应前撤权不交付成功正文，已经提交的写入仍须查原判定。

公共响应是批准视图，`approval_ref`指向内部完整原件，不能把公共投影重算摘要冒充原件；`bundle`保留人员审阅的完整来源内容。读取不授予持续来源当前性、发布或运行资格。实际双进程TLS与SDK已验证五项路由、CSRF／Origin拒绝、错误定义范围、混合上下文额度及撤销后历史查原；首次引导与绑定来源在该HTTP用例中仍为测试装配，不代表生产验收。GUI另通过实际Edge的候选审阅、批准／撤销和已提交响应丢失查原验证；该浏览器场景的首次配对及来源注册仍为测试夹具。

可信服务装配可通过 `app.pairing.eventReceiver.prepare({binding_id,scope,device_id,expected_source_ref,after_sequence,limit})` 读取Controller来源事件。对端须有独立 `events.internal.read` 权限及精确设备范围，分页绑定原来源引用；接收器核验完整来源与事件摘要、连续分页和当前SDK模型。`current(handle)`／`currentInTransaction(handle)` 返回原件副本，`release(handle)` 显式释放；并行读取最多4个、保留句柄最多64个，使用原认证单调域中的2秒读取期限，重读不延长。配对变化、域变化、往返超期、服务关闭或迟到响应均使句柄不可用。

读取区间仅描述本次传输观察，不是来源采集时刻、AgeEvidence或远端持续当前性租约。当前事件保留原 `source_time_ref: null`；该通道尚未写入Seamer规范inbox，不分配摄取游标、不确认来源outbox、不发durable_ingest，也不触发自动化。完整订阅交付、首次观察证据、完整决定集及实际年龄求值仍须接通。

可信服务装配可使用 `app.pairing.subscriptions.create(bindingId,{request_id,consumer_id,purpose,scope})`、`get(bindingId,subscriptionId)` 和 `close(bindingId,subscriptionId,expectedRevision)` 调用Controller Schema 66的订阅初始生命周期。客户端用实际配对mTLS和固定product9 SDK核对请求、响应及完整证书绑定；关闭修订通过查询参数发送。Controller同时核组织消费者批准与独立本地接收政策。新建仅返回building，重复请求查原，关闭和撤权保留不可复活终态。此初始接口不提供快照采用、ACK、inbox或运行触发；attach与下载由下述接口提供，不能用building视图宣称订阅同步完成。

app.pairing.subscriptions.attach(bindingId,view,{replace_existing,connection_attempt_id}) 通过真实mTLS WebSocket附着已批准订阅，核对固定SDK首帧、owner、发布服务及完整代际绑定。成功结果包含ready、attached、connection_attempt_id与显式close方法；不自动重连或重试写入。通过 attempt(bindingId,subscriptionId,attemptId) 查原，snapshot(bindingId,subscriptionId,generation) 和 page(bindingId,subscriptionId,snapshotId,pageIndex,generation) 获取当前连接代际下的固定内容，支持有界完整manifest和页面。WS依赖锁定为ws 8.22.0，未完成握手也计入8连接容量，服务关闭取消握手并关闭连接。这些底层读取只提供经SDK结构核验的正文，完整暂存由下述接收器完成；原子采用、业务inbox和durable_ingest仍待接通，不将下载结果作为运行资格。

`app.pairing.snapshotReceiver.prepare(bindingId,connection)` 接受同一客户端实际附着返回的连接句柄，查询当前订阅与快照上下文后，从头下载固定全页集合。它核验Profile／Selector精确引用、当前图摘要、manifest及页JCS摘要／字节、集合数量、跨页唯一身份及ASCII顺序、来源权威与水位一致性。当前仅支持Controller批准的sources／entity_set／authorized_reference／current组合，其余组合显式拒绝。

接收器以双方合格单调时钟的漂移和量化误差扣除完整往返，固定不晚于原本地样本的期限；后续采集刷新不延长已有句柄。每页和最终读取都重核配对、连接与原租期，最多2个并发下载和8个暂存句柄。`current(handle)`返回副本，`release(handle)`清理暂存；服务关闭使所有句柄失效。快照保留baseline_required及未补齐gap，不触发规则、不写active projection、不发送snapshot.applied，也不作为AgeEvidence或RunGrant。

Schema 4 增加显式快照持久采用。停机后执行 `leafseamer migrate-storage --data-dir <绝对路径> --to-revision 4`；普通启动不升级。迁移在同一事务中保留身份、全部旧迁移和业务原件，再调整元数据版本约束、创建采用表；中途失败回滚。版本1、2、3仍可打开，绑定批准、定义发布及上下文计费在4版继续生效。

采用 `app.pairing.snapshotIngest.apply(handle)` 时，句柄必须来自本机实际接收器且仍在原租期内。短writer事务同时写入完整投影原件、active generation、来源状态及baseline／拒绝floor、审计和通知；延迟外键与封印触发器确保缺写不能提交。事务内继续核验实际TLS连接、配对和时钟，不另起嵌套writer。`lookup(handle)`、本地 `receipt(applyId)`及 `find({owner,subscription_id,snapshot_ref})` 返回已提交原回执；后者允许提交响应丢失且原句柄过期后按原快照身份查原，重放不重复推进generation；历史回执不证明远端当前授权或ACK资格。

同SourceStreamKey的新基线不得低于本机已见上界或拒绝floor；更换epoch关闭旧key并保存其下界，旧key不可重开。原gap摘要和后续观察保留，新快照不凭发布者的synchronized状态恢复本地资格。每次至多32个来源，采用历史最多1024份／64MiB、来源修订最多16384条；单来源保留至多256个gap身份和1024份不同摘要，超额显式拒绝。采用本身不发送snapshot.applied；发布端确认由下述接口显式进行，规则触发和运行恢复仍未接通。

`app.pairing.snapshotConfirmation.send(bindingId,connection,handle)` 只使用本机已提交采用回执；发送前重新核验原句柄租期、配对、实际连接、当前投影及所有来源修订。发布端尚未确认时，连接替换后须用新代际从头验证全页，完全相同的快照可复用原采用回执，不重写来源下界或原记录。方法返回sent只表示已交传输，不表示发布端确认。

`query(bindingId,connection,applyId)` 通过配对 GET 查询原确认，核对当前连接、owner、快照引用和基线标记摘要，返回 pending 或 confirmed。确认响应丢失后可用原采用回执查询。

在配对配置的 `public_api` 中设定 `local_source_subscriptions: true`，可在 Schema 5 或更高版本通过回环 TLS 1.3 的 `POST /auth/local-source-subscriptions/v1` 启动和复查订阅。请求使用发给该 Seamer 的人员 Bearer 令牌，签发方与 Seamer 本地均须授予 `auth.manage`，并核对近期认证。正文为规范 JSON，`operation` 固定为 `connect`，`request` 包含 `request_id`、`consumer_id` 和 Controller 登记范围后返回的完整 `scope`。Controller 仍须为此消费者槽分别批准组织身份和接收政策。管理代理不转发该入口。

以相同请求重投可推进或查询订阅。成功响应 `{result}` 包含 `state`、`subscription_id`、`apply_id` 和 `transport_ready`。 `building` 表示 Controller 正在构建快照，`pending` 表示已有本机采用回执并等待通道就绪，`confirmed` 表示已核对发布端的确认点。只有 `transport_ready: true` 表示当前连接已通过实时传输检查点。断连后使用同一请求和原持久采用回执恢复。订阅撤销、到期或需要重新同步时须由操作员重新核对范围和资格。传输状态本身不提供事件年龄证据或布防资格。

断线后调用 `app.pairing.snapshotConfirmation.resume(bindingId,applyId)`。Seamer 从当前持久投影读取采用回执，重新查询 Controller 已确认的水位，核对来源修订及确认摘要后建立新连接。若要显式替换仍被登记为活动的连接，第三个参数传入 `{replace_existing:true}`。成功返回新连接句柄，订阅处于 `catching_up`。投影已替换、配对失效或确认点不匹配会拒绝恢复。

停机后用 `leafseamer migrate-storage --data-dir <绝对路径> --to-revision 5` 启用持久追赶接收。Schema 5 核验当前配对连接、快照采用回执、来源范围和正文摘要，在 SQLite 提交该帧及接收端首次单调时钟观察后发送 durable_ingest ACK。首次观察引用本机已保存的时钟采集原件，记录接收时使用的启动标识、时钟域和单调时刻。精确重放仍核验并保留原首次观察及原采集引用。事件年龄依据为 `unproven`。当固定区间末尾的 `server.sync.ready` 已持久接收且发布方水位确认后，`snapshotConfirmation.query` 返回 `transport_ready: true`。同一连接代际持久接收检查点后可接受实时帧。新代先按 reconcile 接收历史，旧数据的首次观察保持原样。年龄资格与自动化恢复仍待实现。

Schema 5 的 `snapshotIngest.sourceState` 从已提交的来源投影增量读取当前状态，保留缺口历史，并返回发布方投影修订和摘要。连续水位须对应接收端已核验的来源事件。失步投影保留保护状态，同一来源身份关闭后拒绝新事件和重新采用；后续合法快照须满足已持久化的修订和水位下界。重连仍持原快照采用回执校验。

同一完整来源身份下，已收件的事件 ID 和来源序号在整个 Seamer 库内唯一。新快照和新订阅沿用这组约束。可信本机持久接收组件可按来源和事件 ID 核验读回原事件、首次观察及其交付坐标。

在当前配对和来源绑定下收到超前序号时，Seamer 持久记录缺失区间并标记来源失步。同一事件身份或序号出现不同内容时，来源转入隔离。故障帧不推进接收游标或 ACK。重新采用快照会保留已记录的缺口和本地保护状态。

显式升级到 Schema 5 时，库初始化一个本地接收纪元。已持久收件的来源事件在同一事务中取得 `receive_cursor`，按本库范围和该纪元连续编号。连接、快照、投影更新、检查点及重复交付沿用当前序号。可信接收组件可以核验读回当前基线和原事件的接收坐标。

采用支持精确来源时间读取的 Controller 共享传输组件后，Seamer 会先固定本机首收时刻，再从已配对的 Controller 读回 `Origin.source_time_ref`。原件携带 UTC 引用时，Seamer 在当前配对拥有 `clock.internal.read` 权限的情况下读取完整参考证据包，重算来源锚点及捕获区间的 UTC 上下界。通过校验的原件与本机首次观察一并保存，之后才发送持久确认。无权、读取失败或原件不匹配时，事件仍可记录为仅观察。重投沿用首次观察。接收端参考授时缺失时，年龄仍记为 `unproven`。

Schema 5 的已配置 HTTPS 入口支持 product9 `createRun` 和 `getRun`。创建需要 `run.manage`、有效请求上下文、目标定义权限和精确的已发布制品引用。人员请求还核对 Origin 与 CSRF。登记固定本库接收基线，返回未布防、不可派发的运行。相同幂等键和正文返回原结果，查询需要 `run.read` 及目标定义权限。响应丢失后，原请求人的当前 `request.read` 与 `run.read` 权限可通过 `lookupAutomationRequest` 查回原结果。

创建运行时，Seamer 同事务保存该发布版本在所有程序分支中引用的输入、动作和资源，并将其摘要固定到运行记录。可信本机组件可以在核验 `run.read` 和定义范围后按运行读回原件。

可信本机组件也可按运行解析设备动作目标。Seamer 用发布时固定的动作摘要查找该定义的本地批准，通过已激活的配对登记核对 Controller 服务与库身份，并以双向 TLS 按批准的注册引用重读精确能力版本及完整绑定包。返回前再次核对本地读权限、批准、配对和每次读取的原两秒期限。批准歧义或撤销、远端不可用、注册换代或读回超期时拒绝。每次读取只证明 Controller 在释放响应时完成了复核，组织运行批准还须取得各目标的认可。

停机后可通过 `leafseamer migrate-storage --data-dir <绝对路径> --to-revision 6` 启用运行提案。受信本机装配使用 `authorization.createRunApprovalContext` 和 `runs.createApprovalProposal`，要求当前人员具备 `run.approve`、近期认证及该定义权限。提案固定发布制品、依赖摘要、签发方服务批准、逐目标动作和参数限制，使用原期限内的配对双向 TLS 读回。当前签发方批准只覆盖其本机设备。提案以不可变原件保存；具备当前读取权限的人员可凭原引用查回，或使用原幂等键与请求上下文查原。提案保存后运行仍处于未布防状态。

受信装配可用 `app.pairing.runEndorsementReader.prepare` 从配对 Controller Schema 73 取回指定认可，并核验其提案、目标范围、服务批准及原有效窗口。返回的证据只在发起读取后的两秒内、原配对和本机原单调域中有效，供组织批准事务提交前复核。

停机后显式运行 `leafseamer migrate-storage --data-dir <绝对路径> --to-revision 7` 可启用组织 RunApproval。受信装配使用 `authorization.createApproveRunContext` 与 `runs.approve`，要求人员 `run.approve`、当前定义范围和近期认证。批准重新读取固定源批准和全部目标认可，核验运行修订及原期限，然后原子保存批准、操作回执、审计、状态、通知和新运行修订。当前批准、指定原件和操作可通过 `runs.getCurrentApproval`、`runs.getApprovalRevision`、`runs.getApprovalOperation` 读取，原请求可通过 `runs.lookupApproval` 查回。批准记录的 `applied` 表示 Seamer 已持久化。

配对内部监听通过 `GET /internal/automation/run-approvals/{approval_id}/revisions/{revision}` 释放指定批准。仅批准覆盖、具有 `automation.prepare.read` 的 Controller 服务与数据库域可读取。响应绑定挑战、原摘要与当前配对。`local_eligibility` 表示 Seamer 核对了本地批准状态、当前指针和有效期。目标 Controller 在自己的写事务内核对本地认可和授权交集。撤销传播及 RunGrant 仍待接入。

停机后显式迁移至 Schema 8 可使用 `authorization.createRevokeRunApprovalContext` 和 `runs.revokeApproval`。撤销要求人员具备 `run.approval.revoke`、定义范围及近期认证。Seamer 同事务登记撤销、推进运行修订、阻断新批准证据，并在保护队列中保存逐目标原批准和认可引用。原批准与撤销回执仍可按各自原件查询。有目标的撤销保持 `revoking`，等待 Controller 接收并完成保护。

受信装配可用 `app.pairing.deliverRunApprovalProtection(operationId, targetScope)` 从持久队列交付同一撤销通知。Seamer 根据目标服务和数据库域解析当前配对，发送端需有 `automation.protect`。Controller Schema 74 首次接收时向 Seamer 核对原批准和撤销引用，再持久登记拒绝。同一通知重送查回原回执。回执的 `identity_state` 确认批准已被拒绝，`protection_state` 仍等待围栏。

显式迁移至 Schema 9 后，配对服务启动时自动扫描持久队列、重送尚未保存回执的目标，并持久采用原回执。取得拒绝回执的操作显示 `applying`，该目标仍是待围栏。按请求查原继续返回首次受理回执，当前操作查询显示已有目标回执。实际围栏及全目标完成待接入。

显式迁移至 Schema 10 后，Seamer 的配对监听可接收原目标 Controller 的认可撤销。Seamer 核对配对用途、精确提案、目标范围、认可原件和撤销引用，然后持久拒绝该提案。同一事务会保护已有批准并保存反向通知原回执，已有批准保护操作时复用该操作。回执确认本地拒绝及保护操作身份，资源围栏仍单独收敛。

显式迁移至 Schema 11 后，可信本机业务可保存事件输入绑定批准。人员在当前定义授权和请求上下文内提交候选摘要。Seamer 重新核对两条配对读取、设备会话和审阅原件后，才保存批准与请求回执。撤销追加引用原批准摘要的修订，并登记保护待处理。重投返回原修订，当前查询返回最新状态。事件批准与现有设备绑定批准共用人员授权和上下文额度，各自保存于独立账本。公共 product9 绑定路由继续使用设备绑定账本。

启用 `public_api.local_event_bindings` 后，操作员可通过 `POST /auth/local-event-bindings/v1` 办理事件审批。请求使用回环 TLS 1.3、发给当前 Seamer 的人员 Bearer 令牌和 `Content-Type: application/json`，正文为规范 JSON `{operation, request}`，上限 32 KiB。监听器拒绝带 Cookie、Origin 或代理头的请求。成功响应为 `{result}`。

| `operation` | `request` |
| --- | --- |
| `candidate` | `{definition_id, query}`。`query` 提供 `binding_id`、`scope`、`device_id`、`expected_source_ref` 和 `expected_registration_ref`，两个预期引用可在首次查询时填 `null` |
| `create-context` | `{ttl_ms, max_unique_requests}`，返回批准和撤销使用的 `context_id` |
| `approve` | `{query, approval, context_id}`。`approval` 包含 `request_id`、`approval_id`、`definition_id` 和审阅结果的 `expected_candidate_digest` |
| `inspect` | `{approval_id}`，返回当前批准修订 |
| `revoke` | `{approval, context_id}`。`approval` 包含 `request_id`、`definition_id` 和当前 `approval_ref` |

候选和当前批准查询要求 `auth.read` 及当前定义读取权限。批准和撤销采用两侧的 `auth.manage`、当前定义范围及原近期认证期限，请求上下文绑定同一个令牌。撤销后，用到该批准的新发布请求会被拒绝，原发布制品仍可按权限查询。

停机后执行 `leafseamer migrate-storage --data-dir <绝对路径> --to-revision 12`，可让已撤销的事件或设备绑定触发关联运行批准的保护通知。迁移以原制品记录的精确引用建立索引，并保留原批准、撤销和回执。启动后，Seamer 从账本恢复待处理撤销，复核原件后将受影响的运行批准及逐目标通知同事务入队。原批准及请求仍可查回。Controller 的回执确认已持久拒绝批准身份，设备资源保护保持待完成。普通启动不自动迁移。

Schema 13 增加布防预检原件。停机后执行 `leafseamer migrate-storage --data-dir <绝对路径> --to-revision 13`。可信本机调用方可通过 `runs.recordArmPreflight` 保存本次运行修订、制品、批准、控制和来源观察，凭精确引用经 `runs.getArmPreflight` 查回。`runs.revalidateArmPreflight` 在原启动身份、单调域和截止内重读两端证据，要求与原件一致。到期后仍可按当前读取权限查回原件。当前预检用于记录证据，布防受理仍需完整保护预算和准备资格。

显式迁移至 Schema 14 后，可信本机调用方可使用 `authorization.createArmRunContext`、`runs.armRun` 和 `runs.getActivationSlot`。受理要求人员持有当前 `run.arm` 权限和近期认证，精确预检仍在原期限，来源已同步、传输就绪，控制在自动模式且设备闸受保护。部署装配须提供当前 `armAdmissionPolicy.inspectInTransaction`，以返回受信资格、预算档案、四维容量及本次预留需求。运行的受理记录处于 `reserved`，并固定活动槽、预算、逐 Controller 准备身份及人员证据。运行仍未布防，等待 Controller 准备回执。

Schema 15 增加准备回执账本。停机后执行 `leafseamer migrate-storage --data-dir <绝对路径> --to-revision 15`。受信装配通过 `pairing.submitArmPreparation` 提交已登记目标，响应未知时通过 `pairing.lookupArmPreparation` 按原操作和目标查原。收到经配对核验的原回执后，Seamer 原子保存回执、目标 RunGrant 引用、审计及通知。`runs.armPreparationObservations` 可查回目标的 `pending` 或 `receipt_observed`。该观察保留历史签发事实，运行保持受保护，实际布防、关闭围栏和发送资格仍由后续事务裁决。

停机显式迁移至 Schema 16 后，受信调用方可使用 `runs.completeArmRun(authentication, operation_ref)`，以受理时同一人员和凭据完成普通布防。Seamer 读取全目标当前资格并复核来源、预留预算、活动槽和运行修订，封存结果及通知后将运行推进至 `armed`。定义槽继续持有，活动操作清空，派发保持关闭。原发起人凭当前读取权限可以查回同一完成结果。显式撤销该运行批准时，当前运行以原撤销操作为凭据进入 `paused`。

Seamer 的独立双向 TLS 监听提供 `GET /internal/automation/approval-proposals/{proposal_id}`。配对 Controller 须有 `automation.prepare.read`，且服务和数据库必须与提案的目标条目一致。请求绑定提案摘要和一次挑战；返回原件及引用，认证审计通过引用保存。目标 Controller 在本机配对通道使用 `readRunApprovalProposal`，验证响应挑战、摘要、精确目标和有效期限。读取结果供随后核对认可前提，Controller 仍须检查当前签发方授权及本地可授范围。

### 本机撤销运行批准

启用 `public_api.local_run_approvals` 后，使用 `POST /auth/local-run-approvals/v1`。请求通过回环 TLS 1.3 发送，携带定向当前 Seamer 的人员 Bearer 令牌。`Content-Type` 为 `application/json`，正文为规范 JSON `{operation, request}`，上限 32 KiB。入口拒绝带 Cookie、Origin 或代理头的请求。

| 操作 | 请求 |
| --- | --- |
| `create-revocation-context` | `{ttl_ms, max_unique_requests}`，返回撤销请求的 `context_id` |
| `revoke` | `{approval_id, revocation}`。`revocation` 包含 `approval_ref`、`expected_status_revision`、`reason` 和 `context`。`context` 包含 `context_id` 和 `idempotency_key` |
| `lookup-revocation` | `{approval_id, context_id, idempotency_key}`，查回原请求的摘要和首次受理回执 |
| `inspect-operation` | `{operation_id}`，返回当前操作和逐目标保护进度 |
| `inspect-recovery-evidence` | `{run_id, execution_id}`，返回当前暂停运行中该活动执行的保护回执与命令资源证据 |
| `observe-recovery-protection` | `{run_id, execution_id}`，对同一执行实时复核原撤销通知的全部 Controller 目标，并附各目标的当前结果 |
| `preflight-recovery` | `{run_id, execution_id}`，人员对当前暂停的活动执行同时查验全部撤销目标和逐命令资源，并获取本次只读证据 |

撤销要求当前人员具备 `run.approval.revoke`、目标定义的权限与近期认证。查回请求时使用原身份和上下文，并核对当前读取权限。成功响应为 `{result}`。同一请求重复提交返回原受理回执，修订冲突或同一幂等键对应不同正文时返回 HTTP 409。

有 Controller 目标时，`accepted` 表示 Seamer 已记录撤销并排队发送保护通知，`applying` 表示仍等待部分或全部目标完成保护。通过 `inspect-operation` 跟踪当前进度。撤销不会释放原活动槽，也不会授权新运行。

恢复证据查询要求人员具有当前 `run.read` 和目标定义权限。`observed_protection` 返回 Seamer 已持久采用的回执引用。各动作分别列出原 Controller、设备、提交状态、命令引用、已观察终态及其中的资源证据引用。`applied` 记录保护回执覆盖的代次和设备，恢复前仍须核对当前保护和逐命令资源证据。即使终态记录为 `clear`，缺少资源释放证据也会阻止可选独立动作的后继和执行完成提交。

实时复核使用同样的人员和定义权限。Seamer 按原通知在当前配对上查询每个 Controller，返回前复核权限、配对、执行和已保存回执，且整个查询须落在两秒窗口及原认证样本的有效期内。只有所有目标当前都返回与已保存原件一致的完成回执，`current_protection` 才为 `verified`。未完成、找不到、通信不可用、远端无法确认或回执不一致时，整体返回 `unconfirmed` 并保留逐目标结果。原执行保持暂停，实际资源释放及人员恢复由后续流程核对。

`preflight-recovery` 还按每个已尝试动作的原配对和原提交查询 Controller。只有所有动作都有未提交、永久拒收、从未取得占用或已释放的证据，`current_resources.state` 才为 `verified`。只有当前保护和逐命令资源均已核实，且保护回执覆盖全部动作设备，`recovery_evidence_state` 才为 `verified`。两项证据各自显示。查询保留历史终态和原活动槽，人员恢复需另行授权。

停机执行 `leafseamer migrate-storage --data-dir <绝对路径> --to-revision 40` 可启用持久恢复预览。普通启动沿用数据库已有版本。人员通过同一本机 HTTPS 入口提交以下请求。

| 操作 | 请求 |
| --- | --- |
| `preview-recovery` | `{run_id, execution_id, expected_revision}`，保存当前暂停执行的证据和计划，返回 `preview_ref` |
| `get-recovery-preview` | `{run_id, preview_ref}`，凭当前人员及对象权限查回历史原件 |
| `revalidate-recovery-preview` | `{run_id, preview_ref}`，使用原人员授权在原截止内重读两端证据并验证摘要 |

预览固定完整 Controller 目标、运行和活动槽修订、制品、接收游标、来源及暂停锁存、逐命令证据和计划。计划标出保留的成功步骤、原动作确未写入的普通触发步骤，以及必须停止的步骤。结果未知时，即使占用已核销，计划仍保留停止项。旧排期场次保持原样。重核要求同一服务启动身份和单调域，截止取最初两秒观察窗口与原认证样本有效期的较早值。预览记录为只读历史证据，保存和重核都保持原执行及活动槽。

停机显式迁移至 Schema 41 后，具备签发方和本地 `run.resume` 权限的人员可用近期认证创建恢复上下文，并在原预览有效期内申请恢复。运行读取权限及定义范围仍在受理时复核。

| 操作 | 请求 |
| --- | --- |
| `create-resume-context` | `{ttl_ms, max_unique_requests}` |
| `resume` | `{run_id, reservation}`，其中 `reservation` 包含 `context`、`expected_revision` 和 `preview_ref` |
| `get-recovery-operation` | `{run_id, operation_id}` |
| `lookup-recovery-operation` | `{run_id, context_id, idempotency_key}`，要求当前运行及请求读取权限 |

HTTP 202 表示预览已单次消费。Seamer 原子保存恢复操作、新执行身份、完整待准备目标、活动互斥锁、审计和固定事件通知，并推进运行和活动槽修订。新代次高于原代次及各目标的拒绝下界。同一人员以原键提交相同内容时查回原受理回执，异内容或另一键竞争消费时返回 HTTP 409。受理后运行仍暂停，原执行保持可查。新执行须经过逐目标准备和独立开始事务才可进入派发。

恢复受理后，当前人员可使用 `propose` 和 `approve` 创建绑定该恢复操作的新组织运行批准。提案沿用原获准的服务主体、完整 Controller 目标、设备动作及参数限制。批准请求的 `expected_prior_approval_ref` 引用原已撤销批准，并提交属于新提案的全部目标背书。Seamer 逐目标复核当前设备在自动模式保护态且没有待处理控制切换，随后原子替换运行的当前批准。原批准保留撤销状态并可查到替换引用。对应 Controller 可通过原配对接口查回新批准，恢复操作仍处于 `reserved`，须另行逐目标准备。

停机后用 `leafseamer migrate-storage --data-dir <数据库绝对目录> --to-revision 42` 启用恢复准备。使用支持 `automation-preparation/0.1.0-draft.3` 的配对运行时和 Controller Schema 78 或更新版本。恢复发起人使用近期认证及有效恢复上下文，在同一本机入口调用 `prepare-recovery`，请求为 `{run_id, operation_id, context_id}`。该步骤适用于含后续动作的恢复计划，并要求各目标能够本机核验人员凭据。

Seamer 先原子登记完整请求，再经配对提交。每个目标固定绑定原拒收代次的 RunGrant、新批准、恢复操作、新代次及新执行。新准备授权以五秒为上限，同时受当前权限、配对及各原授权期限约束。提交尝试只登记一次。回包丢失时，再调用 `prepare-recovery` 按原身份查回已提交的回执。具备当前运行读取权限的人员可用 `inspect-recovery-preparation` 查询 `{run_id, operation_id}` 的状态。 `receipts_observed` 表示全部回执原件已保存。运行和设备保护仍保持，后续启用需要核验当前资格并完成开始事务。

显式迁移至 Schema 43 后，恢复发起人可凭近期认证和有效恢复上下文调用 `bind-recovery-execution`，请求为 `{run_id, operation_id, context_id}`。Seamer 要求完整准备回执，按原身份复核各目标当前 RunGrant 资格，并重新核对旧代保护、命令资源、已发布依赖和来源连续性。任一证据缺失、变化或到期时，运行保持暂停。核验通过后，Seamer 原子保存新执行与全目标实际 RunGrant 的绑定原件。用 `get-recovery-execution-binding` 查询 `{run_id, operation_id}` 可查回原件。绑定是历史证据，恢复开始和实际派发仍需当前资格及写入门控。

显式迁移至 Schema 44 后，恢复发起人可调用 `start-recovery-execution`，请求为 `{run_id, operation_id, start}`，其中 `start` 提供 `binding_ref` 和 `context_id`。同一启动域内，Seamer 复核全目标当前资格和原恢复基线，在同一事务固定新执行步骤并推进运行修订。重新执行的动作只引用该目标的新 RunGrant，保留成功的步骤仅引用原完成证据。HTTP 202 返回固定开始记录，查回原记录使用当前运行读取权限。重启后原记录可以查回。仅采用 Schema 44 时，新执行查询返回 `recovery_dispatch_pending`。

显式迁移至 Schema 45 后，Seamer 在开始记录的原截止内为依赖已满足的恢复动作保存步骤资格、参数和唯一提交身份。Controller 使用配对动作协议第 3 版回读该原件，并继续核对当前批准、设备 Ready 与最终写入门控。已保存的旧参数原样继承。运行在当前恢复窗口内可进入 `running`。提交回包未知时，Seamer 暂停执行并查回原命令，重启后继续查原。执行查询的 `dispatch` 字段显示实际提交和裁决。

停机并显式迁移至 Schema 46 后，恢复执行可在原开始窗口内处理新的延时步骤。延时以该恢复执行的开始时刻为锚，前驱成功后记录固定窗口，到点后重新核验当前 RunGrant、配对和来源连续性。直接后继的可提交时间取延时截止、恢复开始截止与其它现行授权期限的最早值。延时达到截止时进入保护。执行查询的 `dispatch.steps` 保留 `delay_window` 和 `delay_elapsed` 原件，重启后可查回。恢复结算及活动槽释放仍待接入。

### 本机排期

显式迁移至 Schema 37 后，在受保护回环监听配置中启用 `public_api.local_schedules`，即可通过 `POST /auth/local-schedules/v1` 发布、激活和读取固定偏移的单次排期。请求使用 TLS 1.3、当前 Seamer 的 Bearer 令牌及规范 JSON `{operation, request}`，成功响应为 `{result}`。

| 操作 | 请求 |
| --- | --- |
| `create-context` | `{operation_id, ttl_ms, max_unique_requests}`，操作 ID 为 `publishSchedule` 或 `activateSchedule` |
| `publish` | `{schedule_id, version}`，版本包含 `context`、`expected_version`、`definition_id`、`definition_version`、`expected_artifact_ref` 和 `items`。首次发布的 `expected_version` 为 `null` |
| `activate` | `{schedule_id, activation}`，激活请求包含 `context`、`expected_revision` 和 `schedule_version_ref`。首次激活的 `expected_revision` 为 `null` |
| `get` | `{schedule_id, version}`，`version` 为 `null` 时返回最新发布版本，并单列当前激活引用 |

单次项包含稳定的 `item_id`、`one_shot_key`、带毫秒的 `local_start`、显式 `±HH:MM` 偏移，以及每个动作步骤的 `step_id`、`role`、`start_offset_ms` 和 `latest_start_offset_ms`。同项改期沿用原场次身份。发布与激活分别需要人员的 `schedule.publish` 和 `schedule.activate` 权限及近期认证，读取要求 `schedule.read`。排期激活和运行布防是独立操作。

排期读取通过 `occurrences` 返回时间保护状态。配置受监督的业务参考时间后，Seamer 在窗口开始前保存最早可能到点的单调时刻。同一次激活后，新的授时样本只能维持或提前该时刻。失信跨过边界会持久登记 `blocked`，已过步骤窗口则登记 `missed`。两种结果均按逻辑场次保留，`tracking` 只表示已登记时间边界，执行受理另行裁决。

替换激活时，Seamer 在同一事务内核对旧场次并检查新版本。旧场次已到最早可能开始时刻时，保护结果随逻辑场次保留，即使新版本将该项移除，之后再加入也沿用原结果。若旧场次尚未终结且缺少可信参考时间，接口返回 `503 CLOCK_UNTRUSTED`，原激活继续有效。

Schema 38 支持原子受理记录。受理事务将场次、激活修订、排期版本、发布制品及首步时间证据与执行封存绑定，并占用同一自动化活动槽。已受理的场次在 `occurrences` 中显示为 `accepted`，改期后仍可查回固定的执行与版本。

供配对 Controller 读取的定时动作证据采用 `leafseamer-action-binding/2` 响应。新受理原件固定场次摘要和步骤窗口索引摘要。动作绑定从受理时的排期版本提取指定步骤的窗口，其它步骤只提供索引中的身份与摘要。

首步的可信 UTC 区间完全进入有效窗口时，当前激活版本的 `occurrences` 可附带 `qualification`。投影检查已布防运行、发布制品、规则分支和首步守卫及参数。定时选支只采用当时可证明的条件，依赖未取得的来源值时保留未知。 `ready_for_preparation` 表示可以开始核验派发所需资格。扫描发现窗口内的活动槽已占用时，将场次持久记为 `blocked`，原因为 `run_busy`。自动消费、逐步派发和设备执行仍需接入。
