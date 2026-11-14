# LeafSeamer / Seamer

> **施工中 / Work in progress。** 已有独立保护态进程、持久服务身份和本机认证时钟接线。已激活配对后的人员身份接收和GUI草稿文档编辑已有实现；首次可信配对引导、完整图形编排、RunGrant和自动化执行尚未完成，当前不提供生产可用服务。

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

Linux构建省略 `--node-library`。也可显式运行 `npm run build -- --native-artifacts <制品绝对目录>`，采用 `runtime-inputs.json` 中固定摘要的现有原生时钟制品；此路径校验制品清单、源码、平台、Node版本和二进制，属于制品采用，不代表本机重新编译。完成构建后可用 `npm pack --ignore-scripts` 打包；安装包运行不依赖邻仓路径。

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

`openStorage().bindPeerPairings({clock, issuer, management})` 是可信本机装配接口。issuer固定指定Controller的服务／签发者／epoch；management必须同步核验当前远端人员身份、定向audience、近期认证和auth.manage授权。缺来源、服务主体、签发者不符或事务末尾人员／会话变化均拒绝并回滚。Seamer只保存已核验人员的外部身份引用，没有identity_issuer表，不自行签发人员身份。

独立TLS监听在pending阶段只开放status、challenges、proofs，不能调用普通introspection或设备接口；activated后可按明确用途调用Controller核验端点；两端须分别stage、证明和批准，单端激活不代表对方已激活。首次可信管理入口政策尚待落实，因此普通CLI与HTTP仍不提供配对批准入口。

`startSeamer` 可由可信本机代码通过 `pairing` 参数装配独立TLS监听，固定本机持久身份和指定签发者，不接受HTTP或环境变量传入管理授权。启动失败关闭已启动的HTTP及TLS、停止认证采样并释放数据库；正常 `close()` 统一关闭HTTP、TLS、认证时钟和文件库，重复关闭返回同一完成结果。

`test/controller-pairing.test.mjs` 在同一测试宿主中运行Controller与Seamer组件、独立文件库和实际双向TLS，验证拒绝服务主体、来源失效、事务末尾换人、双向证明、分别激活和撤销。Controller使用真实本地人员会话，Seamer人员核验和环境资格为合成装配；这不替代真实身份接收或首次配对部署验收。

`test/pairing-process.test.mjs` 另以两个独立Node进程运行Controller与Seamer，覆盖真实双向TLS证明、分别激活、Controller真实人员令牌签发、Seamer核验接收及配对撤销后句柄失效；首次stage与activate的Seamer管理核验仍为合成装配。关闭后在子进程仍存活时检查HTTP与TLS端口均拒绝TCP连接，再退出进程。同一套测试也覆盖正式Controller启动器的HTTPS令牌签发、内部核验、注销自动交付及统一关闭；测试专用 `LEAFSEAMER_CONTROLLER_PACKAGE_UNDER_TEST` 可选择其安装包。设置测试专用 `LEAFSEAMER_PACKAGE_UNDER_TEST` 为安装包绝对目录，可对安装包重复同一路径；它仅由测试夹具读取，不是产品配置。同一进程链还验证本地未批准编辑时拒绝、明确批准后创建／读取草稿、幂等返回原件以及减权后拒绝读取和查原。首次本地管理员批准仍由测试夹具预置；跨主机和首次可信管理引导仍需独立完成。

`startSeamer` 默认将配对管理接到本地授权库的真实身份适配器：管理输入是接收器取得的不透明人员句柄，必须同时具备定向令牌和Seamer本地auth.manage，并处于原近期认证区间。指定issuer、对象身份及本地批准均在同一writer内核对。测试或受信部署装配若显式提供了外部management，可调用 `pairing.useIdentityManagement()` 单向切换到该真实适配器；此接口不创建人员、配对或管理员批准，切换后原来源不能通过它恢复。

撤销当前身份核验配对时，只允许该次revoke事务在末尾核对精确撤销前后原件。内部临时句柄不可由请求构造、复制或跨事务使用，回滚与提交均销毁；原认证时间、近期认证、配置和本地批准仍须有效。它不把已撤销配对重新标为active，不延长身份快照，也不允许撤销完成后的新管理调用。其它配对仍按各自当前状态裁决。

双进程管理用例先明确切换到真实来源，再验证旧合成凭据和仅auth.read令牌拒绝、新配对登记／双向证明／人员批准、未完成配对拒绝，以及当前身份配对的实际撤销。共享组件用例另验证伪造／错引用的临时句柄拒绝、提交末尾失败完整回滚、回滚及提交后的临时句柄失效。首次配对和首个本地管理员仍是测试预置，不能据此宣称首次可信部署已经完成。

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

接收器在当前身份和业务writer核验时查询这些持久拒绝记录。测试覆盖同事务失败／静默忽略回滚、错范围与重复消息，以及真实Controller会话注销、双向TLS投递、接收后发送端尚未登记回执、随后补记相同回执和新进程重开后继续拒绝。首次可信引导、跨主机和生产部署仍待完成。

## 本地批准与草稿授权

`startSeamer` 在配置配对时装配 `authorization` 和 `drafts` 内部服务。授权库以指定issuer／epoch／principal三元组建立独立、不可复用的subject映射，保存不可变批准修订、CAS当前指针、原操作及同writer通知。`authorization.replace` 要求真实身份上限与本地auth.manage交集、human及原近期认证区间；事务末尾再核期限。批准明确权限、all或selected定义范围及认证有效区间，不从管理员名称、远端登录或首次查询自动授予权限。缩权、缩时或缩范围使批准进入pending并立即拒绝使用；保护通知尚未交付确认，pending不能直接重新激活。

`createRequestContext` 要求request.create及所请求草稿操作的两侧权限交集。当前内部实现使用最多60秒、256次新写入的有界context，固定当前SDK选择、boot、请求epoch、主体／会话／凭据、令牌绑定摘要、授权修订和运行域。相同人员的另一令牌不能搬用context，重启后旧context失效；每次消费仍须新鲜身份原件，context期限不延长身份快照。令牌绑定摘要只用于内部关联，不是可兑换凭据。

草稿读写在自己的SQLite writer内同步复核已激活配对、短时身份、本地批准、定义范围及context。新写额度、定义内容／metadata、原请求结果与通知一并提交；失败一并回滚，已提交幂等重试不重复扣费，返回历史原件前仍核当前读取权。`currentInTransaction` 使用现有配对事务核验入口，避免在业务writer内另开事务。

本地管理仍只由可信装配代码调用；context和草稿另可通过显式装配的HTTPS入口访问。普通CLI与健康HTTP没有授权导入、管理员初始化或业务写入口。`test/local-authorization-fixture.mjs`的SQL预置仅用于独立测试，不随包发布。未实现首次引导、服务令牌实际接收、撤销通知确认及完整运行保护，不能把当前内部草稿闭环作为完整自动化或部署批准。

## 公共草稿HTTPS

可信装配可为 `startSeamer` 提供 `publicApi: {cert, key, binding_id, host, port, allowedOrigins}`，其中binding_id固定本端用于指定Controller身份核验的配对记录，host只接受回环地址。公开TLS与内部双向TLS使用独立监听，证书材料由装配方提供；普通CLI默认不启用。监听启动失败统一关闭已启动的HTTP／TLS并释放文件库，正常关闭也包含该HTTPS端点。

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


受监督模块集成已进一步验证：Suite 的 `supervisor_create_module_writable_peer_capabilities` 场景使用实际原生监督器、独立模拟器、Controller 注册及独立 Seamer 安装包。该场景不再预置绑定正文，而是经真实TLS逐字匹配核心生成的注册和绑定；人员批准、断线拒绝、换会话旧注册／旧批准拒绝、显式新批准及运行撤回后的新读取拒绝均通过。测试服务不装配合成 `publicationSource`。此证据关闭模拟器到本地绑定批准的实际集成缺口，首次可信引导、持续来源当前性、发布／执行仍未完成；批准界面现已接入同版GUI。


交互式批准业务可使用 `candidate(authentication,definitionId,sourceQuery)` 查看候选正文，以及 `approveCurrent(authentication,sourceQuery,request)` 在确认后重新读取并提交原审阅摘要。`sourceQuery` 显式包含 `binding_id,scope,device_id,expected_registration_ref`；确认时使用候选的精确注册引用。候选读取先核当前 `auth.read` 和定义范围，批准读取先核人员管理权限；网络等待不持有数据库writer，返回后及提交末尾重新核对同一授权，期间权限或来源变化即拒绝。内部短期句柄使用后立即释放，不交给界面保存；这些方法仍可用于可信应用装配；公共HTTP采用下文的product6操作，批准GUI已接入同版SDK，见GUI的绑定批准使用说明。

`lookup(authentication,{request_id,request_digest,definition_id})` 在当前签发方与本地 `auth.read`、`request.read` 交集和定义范围内，按本人身份查询原请求。`request_digest` 是原批准或撤销请求完整JCS的SHA-256摘要。结果包含原请求定位字段和 `result`；未找到时为null，不证明将来不会提交。已提交请求的精确重放与查原不重新访问远端来源；撤销后的批准查原仍返回第一版历史原件，当前状态须另用 `get` 读取。无写权限时不能借幂等重放恢复写入资格。


配对且采用schema3的公共Seamer HTTPS监听器已提供product6五项操作：`POST /api/v1/definitions/{definition_id}/binding-candidates/query`、`POST .../binding-approvals`、`GET .../binding-approvals/{approval_id}`、`POST .../binding-approvals/{approval_id}/revoke`、`POST .../binding-approval-requests/lookup`。精确模型和类型来自同版SDK，旧product5选择拒绝。批准与撤销要求该监听器签发的上下文及CSRF、精确Origin、真实人员近期认证、双端auth.manage和定义范围；请求结果与上下文额度在同一writer中核验保存。额度由不可变批准／撤销原件中的context_id计入，与同上下文草稿写共享总限额，无额外数据库迁移。服务关闭或响应前撤权不交付成功正文，已经提交的写入仍须查原判定。

公共响应是批准视图，`approval_ref`指向内部完整原件，不能把公共投影重算摘要冒充原件；`bundle`保留人员审阅的完整来源内容。读取不授予持续来源当前性、发布或运行资格。实际双进程TLS与SDK已验证五项路由、CSRF／Origin拒绝、错误定义范围、混合上下文额度及撤销后历史查原；首次引导与绑定来源在该HTTP用例中仍为测试装配，不代表生产验收。GUI另通过实际Edge的候选审阅、批准／撤销和已提交响应丢失查原验证；该浏览器场景的首次配对及来源注册仍为测试夹具。

可信服务装配可通过 `app.pairing.eventReceiver.prepare({binding_id,scope,device_id,expected_source_ref,after_sequence,limit})` 读取Controller来源事件。对端须有独立 `events.internal.read` 权限及精确设备范围，分页绑定原来源引用；接收器核验完整来源与事件摘要、连续分页和当前SDK模型。`current(handle)`／`currentInTransaction(handle)` 返回原件副本，`release(handle)` 显式释放；并行读取最多4个、保留句柄最多64个，使用原认证单调域中的2秒读取期限，重读不延长。配对变化、域变化、往返超期、服务关闭或迟到响应均使句柄不可用。

读取区间仅描述本次传输观察，不是来源采集时刻、AgeEvidence或远端持续当前性租约。当前事件保留原 `source_time_ref: null`；该通道尚未写入Seamer规范inbox，不分配摄取游标、不确认来源outbox、不发durable_ingest，也不触发自动化。完整订阅交付、首次观察证据、完整决定集及实际年龄求值仍须接通。

可信服务装配可使用 `app.pairing.subscriptions.create(bindingId,{request_id,consumer_id,purpose,scope})`、`get(bindingId,subscriptionId)` 和 `close(bindingId,subscriptionId,expectedRevision)` 调用Controller Schema 66的订阅初始生命周期。客户端用实际配对mTLS和固定product8 SDK核对请求、响应及完整证书绑定；关闭修订通过查询参数发送。Controller同时核组织消费者批准与独立本地接收政策。新建仅返回building，重复请求查原，关闭和撤权保留不可复活终态。此初始接口不提供快照采用、ACK、inbox或运行触发；attach与下载由下述接口提供，不能用building视图宣称订阅同步完成。

app.pairing.subscriptions.attach(bindingId,view,{replace_existing,connection_attempt_id}) 通过真实mTLS WebSocket附着已批准订阅，核对固定SDK首帧、owner、发布服务及完整代际绑定。成功结果包含ready、attached、connection_attempt_id与显式close方法；不自动重连或重试写入。通过 attempt(bindingId,subscriptionId,attemptId) 查原，snapshot(bindingId,subscriptionId,generation) 和 page(bindingId,subscriptionId,snapshotId,pageIndex,generation) 获取当前连接代际下的固定内容，支持有界完整manifest和页面。WS依赖锁定为ws 8.22.0，未完成握手也计入8连接容量，服务关闭取消握手并关闭连接。这些底层读取只提供经SDK结构核验的正文，完整暂存由下述接收器完成；原子采用、业务inbox和durable_ingest仍待接通，不将下载结果作为运行资格。

`app.pairing.snapshotReceiver.prepare(bindingId,connection)` 接受同一客户端实际附着返回的连接句柄，查询当前订阅与product8快照上下文后，从头下载固定全页集合。它核验Profile／Selector精确引用、当前图摘要、manifest及页JCS摘要／字节、集合数量、跨页唯一身份及ASCII顺序、来源权威与水位一致性。当前仅支持Controller批准的sources／entity_set／authorized_reference／current组合，其余组合显式拒绝。

接收器以双方合格单调时钟的漂移和量化误差扣除完整往返，固定不晚于原本地样本的期限；后续采集刷新不延长已有句柄。每页和最终读取都重核配对、连接与原租期，最多2个并发下载和8个暂存句柄。`current(handle)`返回副本，`release(handle)`清理暂存；服务关闭使所有句柄失效。快照保留baseline_required及未补齐gap，不触发规则、不写active projection、不发送snapshot.applied，也不作为AgeEvidence或RunGrant。

Schema 4 增加显式快照持久采用。停机后执行 `leafseamer migrate-storage --data-dir <绝对路径> --to-revision 4`；普通启动不升级。迁移在同一事务中保留身份、全部旧迁移和业务原件，再调整元数据版本约束、创建采用表；中途失败回滚。版本1、2、3仍可打开，绑定批准、定义发布及上下文计费在4版继续生效。

采用 `app.pairing.snapshotIngest.apply(handle)` 时，句柄必须来自本机实际接收器且仍在原租期内。短writer事务同时写入完整投影原件、active generation、来源状态及baseline／拒绝floor、审计和通知；延迟外键与封印触发器确保缺写不能提交。事务内继续核验实际TLS连接、配对和时钟，不另起嵌套writer。`lookup(handle)`、本地 `receipt(applyId)`及 `find({owner,subscription_id,snapshot_ref})` 返回已提交原回执；后者允许提交响应丢失且原句柄过期后按原快照身份查原，重放不重复推进generation；历史回执不证明远端当前授权或ACK资格。

同SourceStreamKey的新基线不得低于本机已见上界或拒绝floor；更换epoch关闭旧key并保存其下界，旧key不可重开。原gap摘要和后续观察保留，新快照不凭发布者的synchronized状态恢复本地资格。每次至多32个来源，采用历史最多1024份／64MiB、来源修订最多16384条；单来源保留至多256个gap身份和1024份不同摘要，超额显式拒绝。采用本身不发送snapshot.applied；发布端确认由下述接口显式进行，规则触发和运行恢复仍未接通。

`app.pairing.snapshotConfirmation.send(bindingId,connection,handle)` 只使用本机已提交采用回执；发送前重新核验原句柄租期、配对、实际连接、当前投影及所有来源修订。连接替换后须用新代际从头验证全页，完全相同快照复用原采用回执，不重写来源下界或原记录。方法返回sent只表示已交传输，不表示发布端确认。

`query(bindingId,connection,applyId)` 通过配对GET查原，并核对当前连接、owner、快照引用和独立baseline marker摘要；返回pending或confirmed。查询不重发、不重新采用，不为已被替换的投影返回当前资格。确认响应丢失可只查原；原句柄过期后不借查询延长发送资格。确认后订阅仍为catching_up，来源仍为baseline_required，现阶段未持久记录本地ACK观察，也不提供resume、增量inbox、AgeEvidence或自动化运行恢复。
