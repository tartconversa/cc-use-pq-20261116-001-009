# 展品体验事件湖

本项目使用 FastAPI 和 SQLite 管理场馆、展品、来源契约与事件批次。原始事件保留接收证据，正式报表只读取已发布的日期快照。

## 运行

在预装依赖的环境中，从项目根目录执行：

    uvicorn exhibit_lake.api:app --host 0.0.0.0 --port 8000

默认数据库为本地 SQLite 文件；用 EXHIBIT_LAKE_DB 可指定其他路径。

## 多日原子发布

跨自然日的活动（如周末展期）可以把一组日期放进同一个发布组，整组原子生效：

- `POST /publish-groups/stage`：在单个事务内为整组日期暂存候选快照，任一日期与 `expected_heads` 不符或已有同代候选时整组不落地。
- `POST /publish-groups/{group_id}/publish`：先逐个校验候选，再于单个事务内逐日条件交换 head；任一日期 head 已变化或候选未通过校验时整组回滚，不会部分生效。组内候选在组未决期间不能走单日发布或废弃。
- `GET /publish-groups/{group_id}/report`：活动报表只引用同一发布组内已发布且通过校验的快照，单日后续重新发布不会改变该组视图。
- `GET /publish-groups/{group_id}`、`GET /venues/{venue_id}/publish-groups` 与快照描述中的 `group_id` 支持组与快照之间的互相回查；恢复接口会跳过并标注组绑定候选。

## 测试与构建

    python -m unittest discover -s tests -v
    python -m compileall -q exhibit_lake
    python -m exhibit_lake.demo

以上三条命令依次运行业务测试、编译检查和本地合成数据验收。依赖版本记录在 requirements.txt。
