# Deploy Issues — Encountered & Resolved

记录 download_channel portfolio 在 WSL Ubuntu 上**首次 deploy** 时遇到的所有问题、根因、修复方法,作为后续踩坑参考。

**状态标记**
- ✅ 已修复（commit 到代码里）
- ⏳ Workaround（临时绕过,等条件改善再补）
- ⚠️ Warning 类（不阻塞,需 cleanup）
- ❌ 阻塞中（待修）

---

## 1. `snowsql not found` ✅

**症状**:
```
ERROR: snowsql not found
make: *** [Makefile:22: check-tools] Error 1
```

**根因**: WSL Ubuntu 默认不带 snowsql,它是 Snowflake 自己的 CLI 客户端,无法通过 apt / pip 获取。

**修复**:
```bash
curl -O https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.3/linux_x86_64/snowsql-1.3.2-linux_x86_64.bash
bash snowsql-1.3.2-linux_x86_64.bash    # 两次提示都回车用默认路径
source ~/.profile
```

**预防**: README 的 prerequisite 段应明确列出 snowsql 安装命令,不要假设用户已经有。

---

## 2. `backend-dev.hcl` 混入裸 shell 命令 ✅

**症状**: `terraform init` 报 HCL parse error 在 [terraform/environments/backend-dev.hcl](terraform/environments/backend-dev.hcl) 第 11 行。

**根因**: 文件最后一行残留了 `aform init -reconfigure -backend-config=environments/backend-prod.hcl`——疑似手动编辑时把 shell 命令粘进了配置文件,而且开头 `terr` 也被截掉。HCL parser 直接拒绝。

**修复**: 删掉那一行。

---

## 3. Makefile target 顺序错导致 scripts 桶不存在 ✅

**症状**:
```
=== Uploading Glue scripts to s3://iodp-dc-scripts-dev-<ACCOUNT_ID>/glue/ ===
upload failed: ... NoSuchBucket: The specified bucket does not exist
make: *** [Makefile:45: upload-glue-scripts] Error 1
```

**根因**: [Makefile:53](Makefile#L53) 里 `init` target 把 `upload-glue-scripts` 放在 `deploy-infra-phase1` **之前**,但 scripts 桶是 phase1 的 `module.storage` 才创建的——经典鸡生蛋。

**修复**: 调整 `init` 依赖顺序:
```makefile
# 旧
init: ... check-snowflake upload-glue-scripts deploy-infra-phase1 apply-snowflake-sql ...
# 新
init: ... check-snowflake deploy-infra-phase1 upload-glue-scripts apply-snowflake-sql ...
```

---

## 4. Terraform state 桶不存在（chicken-and-egg）✅

**症状**:
```
Error: Failed to get existing workspaces: S3 bucket "iodp-terraform-state-dev" does not exist.
```

**根因**: Terraform 存储 state 的 S3 桶**必须在 `terraform init` 之前已经存在**——Terraform 自己不会创建它。这是 backend 配置的前置依赖,跟 dropzone 桶一样需要手动 bootstrap。

**修复**:
1. 新增 [Makefile](Makefile) 的 `bootstrap-tf-backend` target,封装 S3 桶创建（versioning + AES256 + public access block）
2. 顺便迁移到 `use_lockfile = true`（S3 native locking),移除 deprecated 的 `dynamodb_table`:
   - [terraform/backend.tf](terraform/backend.tf): 加 `use_lockfile = true`
   - [terraform/environments/backend-dev.hcl](terraform/environments/backend-dev.hcl): 移除 `dynamodb_table` 行
   - [terraform/environments/backend-prod.hcl](terraform/environments/backend-prod.hcl): 同上

**新工作流**: `make init` 之前先跑 `make bootstrap-tf-backend ENV=dev`。

---

## 5. Snowflake provider lookup 失败（子模块缺 `required_providers`）✅

**症状**:
```
Error: Failed to query available provider packages
Could not retrieve the list of available versions for provider hashicorp/snowflake: 
provider registry registry.terraform.io does not have a provider named registry.terraform.io/hashicorp/snowflake
```

**根因**: Terraform 0.13+ 要求**每个使用 non-default provider 的子模块都必须显式声明 `required_providers`**,否则默认查 `hashicorp/<name>`（不存在的命名空间）。[terraform/modules/snowflake/main.tf](terraform/modules/snowflake/main.tf) 和 [terraform/modules/gold_dynamic_tables/main.tf](terraform/modules/gold_dynamic_tables/main.tf) 引用了 `snowflake_*` 资源但没声明 provider source,导致 Terraform 跑去找 `hashicorp/snowflake`。

**修复**: 两个子模块顶部各加一个 `terraform { required_providers { snowflake = { source = "Snowflake-Labs/snowflake", version = "0.98.0" } } }` 块。

---

## 6. Snowflake provider 0.100.0 ValidateProviderConfig panic ✅

**症状**:
```
Error: Plugin did not respond
The plugin encountered an error, and failed to respond to the plugin6.(*GRPCProvider).ValidateProviderConfig call.
```

**诊断流程**:
1. 验证 env vars 齐全（`SNOWFLAKE_USER` / `SNOWFLAKE_ACCOUNT` / `SNOWFLAKE_PASSWORD` / `TF_VAR_snowflake_password`）→ 都齐
2. 验证 snowsql 能连通: `snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -q "SELECT CURRENT_VERSION();"` → 返回 `10.16.101` ✓
3. 结论: 不是凭据问题,是 **provider 0.100.0 自己的 panic bug**

**修复**: pin provider 到 0.98.0（已知稳定）:
- [terraform/versions.tf](terraform/versions.tf): `version = "~> 0.92"` → `version = "0.98.0"`
- 同步改两个子模块 `main.tf` 里的 `required_providers`
- 删 `.terraform/` 和 `.terraform.lock.hcl` 强制重装:
  ```bash
  cd terraform && rm -rf .terraform .terraform.lock.hcl
  terraform init -backend-config=environments/backend-dev.hcl
  ```

**教训**: 公开 portfolio 的 provider 版本**应该锁死 patch**（`= 0.98.0`),而不是用 `~> 0.92` 这种宽松约束——后者会在新 patch 出来时被悄悄升级,可能引入 regression。

---

## 7. WSL 从 `release-assets.githubusercontent.com` 拉 provider 失败 ⏳

**症状**:
```
Error: Failed to install provider
Error while installing snowflake-labs/snowflake v0.98.0: github.com: 
Get "https://release-assets.githubusercontent.com/.../terraform-provider-snowflake_0.98.0_linux_amd64.zip": ...
```

**根因**: 个人网络下 WSL 到 `release-assets.githubusercontent.com`（背后是 Azure Blob Storage CDN）的连接经常被卡。同一 URL 浏览器手动下载没问题,但 Terraform 的 HTTP client 在 packet loss 时容易直接放弃。

**Workaround**:
- 公司网络重试（NVIDIA 内网对 GitHub 友好）
- 换 DNS: `sudo bash -c 'echo "nameserver 1.1.1.1" > /etc/resolv.conf'`
- 手动下载 .zip + 放到 plugin 缓存目录:
  ```bash
  mkdir -p ~/.terraform.d/plugins/registry.terraform.io/snowflake-labs/snowflake/0.98.0/linux_amd64/
  cd ~/.terraform.d/plugins/registry.terraform.io/snowflake-labs/snowflake/0.98.0/linux_amd64/
  unzip ~/Downloads/terraform-provider-snowflake_0.98.0_linux_amd64.zip
  chmod +x terraform-provider-snowflake_v0.98.0
  ```

**状态**: 公司网已验证通过。

---

## 8. Security Group description 含非 ASCII 字符（em-dash） ✅

**症状**:
```
Error: creating Security Group (iodp-dc-glue-dev-...): 
api error InvalidParameterValue: 
Value (Glue ENI security group — self-referencing for Spark shuffle) for parameter GroupDescription is invalid. 
Character sets beyond ASCII are not supported.
```

**根因**: [terraform/modules/networking/main.tf:109](terraform/modules/networking/main.tf#L109) 这个 security group 的 `description` 字段里有一个 **em-dash（— U+2014）**,而 AWS EC2 API 严格只接受 ASCII。这种字符常见于 AI 生成的注释 / 描述,人眼跟连字符 `-` 几乎分辨不出。

**修复**: 把 `—` 换成 ASCII `-`。同时全项目搜一遍 em-dash 防止其他地方也踩。

```bash
# 找出所有用了 em-dash 的代码 / config 文件
grep -rn "—" terraform/ glue/ lambda/ scripts/ snowflake_sql/ Makefile *.md
```

---

## 9. Warning: Snowflake `account` parameter deprecated ⚠️

**症状**:
```
Warning: Argument is deprecated
with provider["registry.terraform.io/snowflake-labs/snowflake"],
on main.tf line 25, in provider "snowflake":
Use `account_name` and `organization_name` instead of `account`
```

**根因**: Snowflake provider 0.95+ 把 `account = "ORG-ACCOUNT"` 拆成了两个独立字段。

**Cleanup（非阻塞）**:
```hcl
# 把 main.tf 里
provider "snowflake" {
  account = var.snowflake_account                # "QNPCBZM-GL59064"
}

# 改成
provider "snowflake" {
  organization_name = var.snowflake_organization # "QNPCBZM"
  account_name      = var.snowflake_account_name # "GL59064"
}
```

需要拆 `var.snowflake_account` 成两个变量。Portfolio 上线前的 cleanup 项目。

---

## 10. Warning: `aws_s3_bucket_lifecycle_configuration` 旧 filter 写法 ⚠️

**症状**:
```
Warning: Invalid Attribute Combination
No attribute specified when one (and only one) of [rule[0].filter,rule[0].prefix] is required.
This will be an error in a future version of the provider.
```

**根因**: AWS provider 新版要求每条 `rule` 必须显式有 `filter` 块或 `prefix` 字段。旧写法（什么都不写,默认对整桶生效）被 deprecated。

**Cleanup**: 每条没有 filter 的 rule 加空 filter 块（等价于"对所有 object 生效"）:
```hcl
rule {
  id     = "..."
  status = "Enabled"
  filter {}    # ← 新增,显式表达"对全桶生效"
  ...
}
```

---

## 11. Warning: Snowflake provider 命名空间迁移 ⚠️

**症状**:
```
Warning: The remote registry returned warnings for registry.terraform.io/snowflake-labs/snowflake:
For users on Terraform 0.13 or greater, this provider has moved to snowflakedb/snowflake.
```

**根因**: Snowflake 官方把 provider 从 `Snowflake-Labs` 组织迁移到 `snowflakedb` 组织。旧 source 仍然能用但 deprecated。

**Cleanup**: 3 处 `source = "Snowflake-Labs/snowflake"` 改为 `source = "snowflakedb/snowflake"`。但要先确认新 namespace 下还有 0.98.x（兼容现有代码）还是只有 1.x（有 breaking changes,需要大改资源定义）。

---

## 12. Snowflake email 验证邮件未送达（Outlook） ⏳

**症状**: `ALTER USER FREDRIC SET EMAIL='fredric2010@outlook.com'` 跑成功,但 Outlook 收件箱 / 垃圾邮件 / Other tab 都没有 Snowflake 发的验证邮件。后续创建 `NOTIFICATION INTEGRATION` 报:
```
Email recipients in the given list at indexes [1] are not allowed. 
Either these email addresses are not yet validated or do not belong to any user in the current account.
```

**根因**:
- outlook.com 对 `*@snowflake.com` 类自动邮件打分严,常在 SMTP 层就拒收
- 新版 Snowflake 的"验证邮件"行为不稳定——有时 ALTER USER 完全不触发,只在 NOTIFICATION INTEGRATION 第一次 send 时才发

**Workaround**: 跳过 `08_freshness_alert.sql`:
```bash
mv snowflake_sql/08_freshness_alert.sql snowflake_sql/_08_freshness_alert.sql.skip
```
`apply_snowflake_sql.sh` 的 glob `[0-9]*.sql` 不匹配 `_` 开头的文件名,自动 skip。

**配套修复（issue #13）**: 脚本的 preflight 1 现在已经跟 `.skip` 联动——文件不存在就跳过邮箱检查。所以光 mv 文件就够了，不需要再单独绕过 preflight。

**完整恢复路径**（三选一）:
- 改用 NVIDIA 工作邮箱（signup 时已自动验证）→ 改 dev.tfvars 的 `alarm_email` + `ALTER USER ... SET EMAIL=...`
- 改用 Gmail（接收率高于 outlook）
- 在 Snowflake UI 上手动找 Resend Verification 按钮

恢复后:
```bash
mv snowflake_sql/_08_freshness_alert.sql.skip snowflake_sql/08_freshness_alert.sql
FORCE=1 make apply-snowflake-sql ENV=dev
```

---

## 13. `apply_snowflake_sql.sh` preflight 1 没跟 `.skip` 联动 ✅

**症状**: issue #12 的 workaround 把 `08_freshness_alert.sql` mv 成了 `.skip`，但 phase 2 仍然报：
```
ERROR: ALERT_EMAIL 'fredric2010@outlook.com' is not bound to any Snowflake user.
make: *** [Makefile:108: apply-snowflake-sql] Error 1
```

**根因**: [scripts/apply_snowflake_sql.sh:102-146](scripts/apply_snowflake_sql.sh#L102-L146) 的 preflight 1 是**无条件** `SHOW USERS` + 匹配 email 列，不存在就 `exit 1`。但项目里唯一用 `ALERT_EMAIL` / `SYSTEM$SEND_EMAIL` 的 SQL 就是 `08_freshness_alert.sql`——它已经被 `.skip` 排除了，部署 loop 根本不会跑到任何引用邮箱的语句。preflight 守的是**已经不会被部署的功能**。

`grep ALERT_EMAIL snowflake_sql/*.sql` 命中 0 个有效 SQL，验证邮箱绑定是死代码。

**修复**: preflight 1 加 `.skip` 联动 ([scripts/apply_snowflake_sql.sh:102-108](scripts/apply_snowflake_sql.sh#L102-L108))：
```bash
if [[ ! -f "${SQL_DIR}/08_freshness_alert.sql" ]]; then
    echo "=== Preflight 1/2: SKIPPED — 08_freshness_alert.sql not present ==="
    echo "  (no SQL in this deploy uses SYSTEM\$SEND_EMAIL)"
    EMAIL_STATUS="REGISTERED"
else
    # 原检查保留
    ...
fi
```

恢复 `08_freshness_alert.sql` 后 preflight 自动回来，不需要再改脚本。

**与 AWS 侧告警的关系**: CloudWatch / Glue 失败告警走 SNS subscription confirm（[modules/observability/main.tf](terraform/modules/observability/main.tf)），跟 Snowflake `SYSTEM$SEND_EMAIL` 是两条完全独立的通道。outlook 收 SNS 没问题，所以 phase 3 / 4 不受影响。

---

## 14. AWS provider 被 pin 到比 state 旧的版本，plugin panic ✅

**症状**（容易跟 issue #6 混淆，但根因完全不同）:
```
Warning: Failed to decode resource from state
  Error decoding "module.storage.aws_sns_topic.silver_notifications":
    unsupported attribute "fifo_throughput_scope"
  Error decoding "module.dynamodb.aws_dynamodb_table.checkpoint":
    unsupported attribute "recovery_period_in_days"

Error: Plugin did not respond
  with provider["registry.terraform.io/hashicorp/aws"]
  The plugin encountered an error, and failed to respond to the plugin.(*GRPCProvider).ValidateProviderConfig call.
```

**和 issue #6 的区别**: #6 是 **Snowflake** provider 在 0.100.0 自己有 panic bug；这次是 **AWS** provider 被人为 pin 到了一个**比当前 state 旧**的版本，状态文件里有 provider 不认识的 attribute → 解码崩溃 → gRPC 不响应。两者错误消息长得几乎一样，但触发条件相反——#6 是太新，#14 是太旧。

**根因**: [terraform/versions.tf](terraform/versions.tf) 把 AWS provider 从 `~> 5.40`（之前 deploy 时被解析到 ≥5.100）改成硬 pin `5.95.0`。但现有 state 是用 ≥5.100 写的，里面带了：
- `aws_sns_topic.fifo_throughput_scope` —— provider 5.97+ 才有
- `aws_dynamodb_table.recovery_period_in_days`（PITR 可配 retention）—— provider 5.100+ 才有

5.95.0 完全不认识这两个字段，refresh 阶段 decode state 时 plugin 直接 panic。

**诊断流程**:
1. 看 warning：state 里有 `unsupported attribute` → 立即怀疑是 schema 不匹配
2. `git diff terraform/versions.tf` 确认 AWS provider 版本被改动过
3. 那两个 attribute 都是 provider 5.97 / 5.100 才加的 → 确认是**降级**而非升级问题

**修复**: pin 到能 decode 现有 state 的版本：
```diff
 aws = {
   source  = "hashicorp/aws"
-  version = "5.95.0"
+  version = "5.100.0"
 }
```
然后 `rm -rf terraform/.terraform terraform/.terraform.lock.hcl && make init ENV=dev`。

**教训（对 issue #6 的补充）**: #6 的"pin 死 patch"原则应用到 AWS 上时**版本号必须是当时实际跑通的那个**，不能随便挑一个旧版本号 pin。判断流程：
1. 上一次跑通的 deploy 用的是什么约束（`~> 5.40`）→ Terraform Registry 解析出来是哪个具体版本（可看 `.terraform.lock.hcl`，但删过的话只能从 state 反推）
2. State 里出现哪些"超前 attribute"反推 provider 最低版本下界
3. 取 ≥ 下界的最近 minor 作为新的硬 pin

简言之：**收紧 provider 版本约束时，pin 到当时实际解析到的版本，而不是凭印象选**。

---

## 15. snowsql 和 Terraform Snowflake provider 用不同的环境变量名 ✅

**症状**: `apply_snowflake_sql.sh` 进交互 prompt 后失败:
```
Account: User: Password:
250001 (n/a): Could not connect to Snowflake backend after 2 attempt(s). Aborting
```
但 `make check-snowflake` 是过的（说明 `SNOWFLAKE_USER` / `SNOWFLAKE_PASSWORD` / `SNOWFLAKE_ACCOUNT` 已设）。

**根因**: 同一份凭据被两个工具用**不同的环境变量名**读取:
- **Terraform Snowflake provider** 读 `SNOWFLAKE_USER` / `SNOWFLAKE_PASSWORD` / `SNOWFLAKE_ACCOUNT`
- **snowsql CLI** 读 `SNOWSQL_USER` / `SNOWSQL_PWD` / `SNOWSQL_ACCOUNT` 或 `~/.snowsql/config`

[Makefile:32-37](Makefile#L32-L37) 的 `check-snowflake` 只验证 Terraform 那套；[apply_snowflake_sql.sh:264](scripts/apply_snowflake_sql.sh#L264) 调 snowsql 时不传任何 flag，依赖 snowsql 自己读 env 或 config。两边都没有时 snowsql 退到交互 prompt，但 stdin 被 `sed | snowsql` 喂了 SQL，prompt 收到空字符串。

**坑中坑**: `apply_snowflake_sql.sh` 的 preflight 2 仍然报「✓ no prior stateful objects detected (fresh deploy)」，让人以为 Snowflake 连得上。其实 [scripts/apply_snowflake_sql.sh:85-93](scripts/apply_snowflake_sql.sh#L85-L93) 的 `probe_snowsql` 用 `2>&1 || true` 吞掉了连接错误，提取不到 `::COUNT::` 标记就默认 0。preflight 分不清「没有对象」和「根本没连上」（这是 issue #13 同一类「preflight 守的不是它以为在守的东西」的设计缺陷）。

**修复**: 在 [scripts/apply_snowflake_sql.sh:81-103](scripts/apply_snowflake_sql.sh#L81-L103) 加单向映射，让用户只维护 `SNOWFLAKE_*` 一套:

```bash
: "${SNOWSQL_ACCOUNT:=${SNOWFLAKE_ACCOUNT:-}}"
: "${SNOWSQL_USER:=${SNOWFLAKE_USER:-}}"
: "${SNOWSQL_PWD:=${SNOWFLAKE_PASSWORD:-}}"
export SNOWSQL_ACCOUNT SNOWSQL_USER SNOWSQL_PWD

if [[ -z "${SNOWSQL_ACCOUNT}" || -z "${SNOWSQL_USER}" || -z "${SNOWSQL_PWD}" ]]; then
    echo "ERROR: snowsql credentials missing..." >&2; exit 1
fi
```

`Makefile` 的 `check-snowflake` 保持只检查 `SNOWFLAKE_*`，跟 Terraform provider 对齐，单一来源收敛在脚本里。

**教训**: 任何场景下「同一份凭据被多个工具用不同变量名读取」都是坑。先 grep 工具的官方文档而不是凭直觉以为 `SNOWFLAKE_*` 一套通用。

---

## 16. SQL 字符串字面量里的 em-dash 触发 snowsql Unicode 异常 ✅

**症状**（看似随机 SQL 编译错误）:
```
001835 (42601): SQL compilation error: error line 2 at position 12
Invalid Unicode string literal; low surrogate '\uDCE2' must be preceded by a high surrogate ('\uD800'-'\uDBFF').
```

**根因**: `\uDCE2` 是 **Python "surrogateescape" 错误处理**把 UTF-8 字节 `0xE2` 编进 low surrogate range (`\uDC00-\uDCFF`) 的产物。`0xE2` 是 em-dash (`—` = `E2 80 94`) 的首字节。snowsql 的 Python connector 在某些路径下把 UTF-8 当 latin-1/cp1252 解码，对 multi-byte 字符 escape 失败，错误地生成 lone low surrogate，Snowflake 服务端拒收。

涉及的文件（**只有字符串字面量内的 em-dash 会炸**，SQL 行注释 `-- ...` 里的 em-dash 由 lexer 直接跳过不会触发）:
- [snowflake_sql/01_database_schemas.sql:25](snowflake_sql/01_database_schemas.sql#L25): `COMMENT = 'Snowpipe load role — INSERT only'`
- [snowflake_sql/03_silver_table.sql:27](snowflake_sql/03_silver_table.sql#L27): `COMMENT = 'Download Channel unified wide table — loaded by Snowpipe'`
- [snowflake_sql/05_gold_dynamic_tables.sql:56](snowflake_sql/05_gold_dynamic_tables.sql#L56): `COMMENT = 'Paid vs organic downloads trend — trailing 30 days'`

**修复**: 把这 3 处字符串里的 `—` 换成 ASCII `-`。注释里的（包括中文「——」标点）保留。

```bash
# 找字符串字面量内的 em-dash (跟全局 em-dash 不同)
grep -Pn "'[^']*\xe2\x80\x94[^']*'" snowflake_sql/*.sql
```

**和 issue #8 的关系**: #8 只覆盖了 AWS API 拒 non-ASCII (Security Group description)；#16 是同一根因的 **SQL 字符串字面量** 版本。教训升级为: **em-dash / smart quotes / curly apostrophes 等 typographic 字符在任何被外部解析器消费的字符串里都有风险**，不局限于 AWS API。

**预防**: AI 生成代码时极易产生 em-dash 替代 ASCII hyphen (人眼分辨不出)，应该在 pre-commit hook 加一条扫描:
```bash
grep -Pn "[\xe2][\x80][\x90-\x9f]" terraform/ glue/ lambda/ scripts/ snowflake_sql/ Makefile *.md && exit 1
```

---

## 17. Snowpipe AWS IAM role 没在 phase 1 创建，04_pipe.sql AssumeRole 被拒 ✅

**症状**:
```
003167 (42601): Error assuming AWS_ROLE:
User: arn:aws:iam::782091841703:user/q7yp1000-s is not authorized to perform:
  sts:AssumeRole on resource: arn:aws:iam::165518479671:role/iodp-dc-snowpipe-s3-dev
```

**根因**: Phase 排序 bug。Snowflake 的 STORAGE INTEGRATION 在 [terraform/main.tf:155-157](terraform/main.tf#L155-L157) 用**硬编码 role ARN** 打破 Snowflake↔AWS 循环依赖:
```hcl
snowpipe_iam_role_arn = "arn:aws:iam::${var.aws_account_id}:role/iodp-dc-snowpipe-s3-${var.environment}"
```
注释说「snowpipe 模块会创建这个 role」，但 [Makefile 的 phase1](Makefile) 只 target 了 `networking / storage / dynamodb / glue_catalog / snowflake`，**没有 `module.snowpipe`**。

部署顺序变成:
```
phase1 (no snowpipe role)
  → apply-snowflake-sql  ← 04_pipe.sql 让 Snowflake AssumeRole 一个还不存在的 role
phase2 (creates snowpipe role)  ← 太晚了
```

AWS STS 对「role 不存在」和「trust policy 拒绝」**返回同样的 not authorized 错误**（不泄露 role 是否存在），所以错误信息容易误导。

**修复**: 把 `module.snowpipe` 和 `module.observability` 加进 phase 1 的 `-target` 列表 ([Makefile](Makefile)):
```diff
   -target=module.snowflake \
+  -target=module.observability \
+  -target=module.snowpipe \
   -auto-approve
```
`module.observability` 是 snowpipe 的依赖 (snowpipe 引用 `module.observability.sns_alert_topic_arn` 做 CloudWatch alarm)，加 `-target=module.snowpipe` 会自动拉，但显式列出更清晰。

**注意 race window**: IAM trust policy 创建后有 5-30 秒的 AWS 端 propagation 延迟。理论上 phase 1 一结束就立刻 CREATE PIPE 可能撞上「role 已存在但 trust 还没全球传播」的窗口。实际上 phase 1 收尾 + 进 apply-snowflake-sql 的两个 preflight 加起来通常超过 30 秒，不会撞。撞到的话重跑 `make apply-snowflake-sql ENV=dev` (CREATE OR REPLACE 全幂等)。

---

## 18. WSL 跑 terraform 时 NTFS-via-WSL exec plugin 不稳定 ✅

**症状**（每次跑崩的 plugin 不一样）:
```
Error: Failed to load plugin schemas
  Could not load the schema for provider registry.terraform.io/hashicorp/archive:
    Plugin did not respond: ... GetProviderSchema call
  Could not load the schema for provider registry.terraform.io/snowflake-labs/snowflake:
    Plugin did not respond: ... GetProviderSchema call
```
重跑一次:
```
  Could not load the schema for provider registry.terraform.io/hashicorp/aws:
    Plugin did not respond: ... GetProviderSchema call  ← 这次是 AWS 崩
```

**和 #6 / #14 的区别**: #6 是 Snowflake provider 自己 0.100.0 panic bug; #14 是 AWS provider 被 pin 到比 state 旧；#18 是**所有 plugin 都可能随机崩，每次崩的不一样**——根因不在 plugin 本身。

**根因**: 工作目录在 `/mnt/c/code1/download_channel/`（Windows NTFS），WSL 从这里加载 Linux ELF terraform plugin 时走 9P/DrvFs 协议:
- AWS provider 200+ MB 单二进制，每次 plan 都 fork 一个新进程加载整个 ELF
- 9P 协议在 NTFS 上短时阻塞 (Windows Defender 实时扫描、文件锁、page cache miss) 会让 plugin 进程启动 timeout
- terraform 主进程等不到 plugin 应答 gRPC，报 `Plugin did not respond`
- 每次崩哪个 plugin 看运气（哪个进程启动时撞到 9P 短停）

跟 issue #7 (WSL 拉 provider 失败) 是同一土壤的不同环节: #7 是**下载**阶段不稳定，#18 是**exec** 阶段不稳定。

**修复**: 把 terraform 的 per-working-directory 数据（plugins / module cache / state cache）搬到 WSL 原生 ext4，用 `TF_DATA_DIR` 重定向:

```bash
mkdir -p ~/.terraform-data/download-channel
export TF_DATA_DIR=~/.terraform-data/download-channel

# 加到 ~/.bashrc 一劳永逸
echo 'export TF_DATA_DIR=~/.terraform-data/download-channel' >> ~/.bashrc

# 清掉 NTFS 上残留的 .terraform
rm -rf terraform/.terraform terraform/.terraform.lock.hcl

# 重跑
make deploy-infra-phase1 ENV=dev
```

代码、Makefile、IDE 都不用动 — `TF_DATA_DIR` 只影响 plugin/cache 位置。Lock file (`.terraform.lock.hcl`) 还在 NTFS 上但它是纯文本，跨 fs 无问题。

**Plan B (彻底)**: 整个 repo 用 `rsync -a` 搬到 `~/projects/download_channel/`，VSCode 用 Remote-WSL 扩展打开 ext4 路径。state 在 S3 backend，跟工作目录在哪无关。

---

## 19. `archive_file` data source 用相对路径在 plan-time 解析失败 ✅

**症状**: terraform plan 阶段，3 个 Lambda 的 `archive_file` 全报「missing directory」，但目录实际存在:
```
Error: Archive creation error
  with module.observability.data.archive_file.dlq_report,
  on modules/observability/main.tf line 138
  error creating archive: could not archive missing directory:
    modules/observability/../../../lambda/dlq_weekly_report
```
直接 `ls modules/observability/../../../lambda/dlq_weekly_report/` 能看到 `handler.py`。路径数学正确，文件可读，但 archive plugin 找不到。

**根因（推测）**: archive provider v2.4.2 把 `${path.module}` 拼出来的相对路径发给 plugin 进程后，plugin 自己 normalize `../../..` 时不稳定。也可能是 issue #18 的同一土壤——plugin 进程从 NTFS 9P 短停时部分系统调用失败。

**修复**: 3 处 `source_dir` 用 `abspath()` 包一下，在 terraform 主进程内就把路径解析成绝对路径，发给 plugin 时不再依赖 plugin 自己的相对路径逻辑 ([terraform/modules/observability/main.tf:140,245,349](terraform/modules/observability/main.tf)):

```diff
- source_dir = "${path.module}/../../../lambda/dlq_weekly_report"
+ source_dir = abspath("${path.module}/../../../lambda/dlq_weekly_report")
```

**性质**: 防御性修法。即使根因是 #18 的 NTFS 抖动，`abspath()` 也能让 plugin 少一步路径解析，减少出错面。

---

## 20. Lambda `reserved_concurrent_executions` 在 quota=10 的账户上必拒 ✅

**症状**:
```
Error: setting Lambda Function (iodp-dc-dlq-weekly-report-dev) concurrency:
  InvalidParameterValueException: Specified ReservedConcurrentExecutions for function
  decreases account's UnreservedConcurrentExecution below its minimum value of [10].
```
3 个 Lambda 各申 `reserved_concurrent_executions = 1`，加起来才 3，看起来无害但 AWS 还是拒。

**根因**: AWS 账户级 Lambda 配额规则:
- 总配额 = Reserved + Unreserved
- AWS **硬性保证 Unreserved ≥ 10**（防账户内其他 Lambda 被饿死）
- 个人 / 沙箱 / 久未用账户的默认 `ConcurrentExecutions` 经常被 AWS 自动降到 **10**（不是文档说的 1000）
- `10 - (任何 reservation > 0) < 10` → 拒

确认账户配额:
```bash
aws lambda get-account-settings --region ap-southeast-1 \
  --query 'AccountLimit.ConcurrentExecutions'
# 返回 10 = 已被降到沙箱档
```

**修复**: 删 3 处 `reserved_concurrent_executions = 1` ([terraform/modules/observability/main.tf:154 / 259 / 363](terraform/modules/observability/main.tf))。Lambda 退回到默认（不限 reservation，用账户共享池）。

**为什么 dev 可以删**: 这 3 个 Lambda 全是 cron / EventBridge 触发（每周 / 每小时），自然并发 1-2 个，throttling 是 prod-only concern。dev portfolio 没真流量，reservation 是浪费配额。

**Prod 怎么加回来（cleanup 项）**: 按环境条件加:
```hcl
reserved_concurrent_executions = var.environment == "prod" ? 1 : -1
# -1 = unreserved (默认行为, 不占 reservation)
```

**长期解**: AWS Console → Service Quotas → Lambda → "Concurrent executions" → Request increase 申请回 1000，一般几小时批。但不阻塞 dev deploy。

---

## 总结: 这次 deploy 暴露的设计 / 文档 gap

| Gap | 状态 |
|---|---|
| `bootstrap-tf-backend` 缺失 | ✅ 已加 Makefile target + README step |
| Makefile target 顺序错（upload-glue-scripts vs phase1） | ✅ 已修复 |
| 子模块 `required_providers` 缺失 | ✅ 已修复 |
| Provider 版本约束太宽松（`~> 0.92` 允许踩 0.100 bug） | ✅ pin 到 0.98.0 |
| `dynamodb_table` 已 deprecated | ✅ 迁移到 `use_lockfile=true` |
| `Snowflake-Labs` namespace deprecated | ⚠️ 待 cleanup |
| `account` provider 参数 deprecated | ⚠️ 待 cleanup |
| `aws_s3_bucket_lifecycle_configuration` 旧 filter 写法 | ⚠️ 待 cleanup |
| Security Group description 含 em-dash | ✅ 改为 ASCII `-` |
| Snowflake email 验证不通过 | ⏳ workaround,后续换邮箱 |
| `apply_snowflake_sql.sh` preflight 1 没跟 `.skip` 联动 | ✅ 已加联动判断 |
| AWS provider 被 pin 到比 state 旧（5.95.0 < state 写入版本 ≥5.100） | ✅ pin 到 5.100.0 |
| snowsql 用 `SNOWSQL_*` 不读 `SNOWFLAKE_*`，凭据映射缺失 | ✅ 脚本顶部加单向映射 |
| SQL 字符串字面量内 em-dash 触发 snowsql Unicode panic | ✅ 3 处改 ASCII (issue #8 的 SQL 版本) |
| Snowpipe IAM role 没在 phase 1 创建，04_pipe.sql AssumeRole 被拒 | ✅ phase1 加 `module.snowpipe + observability` |
| WSL NTFS 上 exec Linux ELF plugin 不稳定（每次崩的 plugin 不同） | ✅ `TF_DATA_DIR` 重定向到 ext4 |
| `archive_file` 相对路径在 plan-time 解析失败（可能是 #18 副作用） | ✅ 防御性 `abspath()` 包路径 |
| Lambda `reserved_concurrent_executions = 1` 在 quota=10 账户上必拒 | ✅ 3 处删除（dev 不需要 throttling） |

## 接下来的步骤

1. ~~修 issue #8 em-dash~~ ✅ 已修
2. ~~修 issue #13 preflight 1 跟 `.skip` 联动~~ ✅ 已修
3. ~~修 issue #14 AWS provider 版本降级~~ ✅ 已 pin 到 5.100.0
4. ~~修 issue #15 snowsql 凭据映射~~ ✅ 已在 `apply_snowflake_sql.sh` 顶部加映射
5. ~~修 issue #16 SQL 字符串字面量 em-dash~~ ✅ 3 处已改 ASCII
6. ~~修 issue #17 Snowpipe phase 排序~~ ✅ phase1 加 module.snowpipe / observability
7. ~~修 issue #18 NTFS plugin 不稳定~~ ✅ 用 `TF_DATA_DIR=~/.terraform-data/download-channel`
8. ~~修 issue #19 archive_file 路径~~ ✅ `abspath()` 包裹
9. ~~修 issue #20 Lambda concurrency~~ ✅ 删 3 处 reservation
10. Deploy 跑通后再依次处理 ⚠️ 三个 deprecation warning（cleanup pass）
11. Email 验证那个晚点解决,不阻塞主流程
12. (新) 加 pre-commit hook 防 typographic 字符 (issue #16 预防)，AI 生成代码经常踩
