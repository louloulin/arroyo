use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

/// 图表类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChartType {
    /// 折线图
    Line,
    /// 柱状图
    Bar,
    /// 饼图
    Pie,
    /// 面积图
    Area,
    /// 散点图
    Scatter,
    /// 热力图
    Heatmap,
    /// 表格
    Table,
    /// 仪表盘
    Gauge,
    /// 单值
    SingleValue,
}

/// 时间范围
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TimeRange {
    /// 最近 15 分钟
    Last15Minutes,
    /// 最近 1 小时
    Last1Hour,
    /// 最近 6 小时
    Last6Hours,
    /// 最近 12 小时
    Last12Hours,
    /// 最近 24 小时
    Last24Hours,
    /// 最近 2 天
    Last2Days,
    /// 最近 7 天
    Last7Days,
    /// 最近 30 天
    Last30Days,
    /// 自定义
    Custom,
}

/// 刷新间隔
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RefreshInterval {
    /// 关闭
    Off,
    /// 5 秒
    Seconds5,
    /// 10 秒
    Seconds10,
    /// 30 秒
    Seconds30,
    /// 1 分钟
    Minute1,
    /// 5 分钟
    Minutes5,
    /// 15 分钟
    Minutes15,
    /// 30 分钟
    Minutes30,
    /// 1 小时
    Hour1,
    /// 2 小时
    Hours2,
    /// 1 天
    Day1,
}

/// 图表数据点
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DataPoint {
    /// 时间戳（Unix 时间戳，毫秒）
    pub timestamp: u64,
    /// 值
    pub value: f64,
}

/// 图表数据系列
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DataSeries {
    /// 系列名称
    pub name: String,
    /// 数据点
    pub data: Vec<DataPoint>,
    /// 标签
    pub labels: HashMap<String, String>,
}

/// 图表配置
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChartConfig {
    /// 图表 ID
    pub id: String,
    /// 图表标题
    pub title: String,
    /// 图表描述
    pub description: Option<String>,
    /// 图表类型
    pub chart_type: ChartType,
    /// 指标查询
    pub metric_query: String,
    /// 刷新间隔
    pub refresh_interval: RefreshInterval,
    /// 是否显示图例
    pub show_legend: bool,
    /// 是否堆叠
    pub stacked: bool,
    /// 是否填充
    pub fill: bool,
    /// 是否显示点
    pub show_points: bool,
    /// 是否显示阈值
    pub show_thresholds: bool,
    /// 阈值
    pub thresholds: Vec<f64>,
    /// 阈值颜色
    pub threshold_colors: Vec<String>,
    /// 单位
    pub unit: Option<String>,
    /// 最小值
    pub min: Option<f64>,
    /// 最大值
    pub max: Option<f64>,
    /// 小数位数
    pub decimals: Option<u32>,
    /// 自定义配置
    pub custom_config: Option<HashMap<String, String>>,
}

/// 仪表盘布局项
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DashboardLayoutItem {
    /// 图表 ID
    pub chart_id: String,
    /// X 坐标
    pub x: u32,
    /// Y 坐标
    pub y: u32,
    /// 宽度
    pub width: u32,
    /// 高度
    pub height: u32,
}

/// 仪表盘配置
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DashboardConfig {
    /// 仪表盘 ID
    pub id: String,
    /// 仪表盘标题
    pub title: String,
    /// 仪表盘描述
    pub description: Option<String>,
    /// 是否为系统仪表盘
    pub is_system: bool,
    /// 所有者
    pub owner: Option<String>,
    /// 标签
    pub tags: Vec<String>,
    /// 时间范围
    pub time_range: TimeRange,
    /// 自定义开始时间（Unix 时间戳，毫秒）
    pub custom_start_time: Option<u64>,
    /// 自定义结束时间（Unix 时间戳，毫秒）
    pub custom_end_time: Option<u64>,
    /// 刷新间隔
    pub refresh_interval: RefreshInterval,
    /// 图表配置
    pub charts: Vec<ChartConfig>,
    /// 布局
    pub layout: Vec<DashboardLayoutItem>,
    /// 变量
    pub variables: HashMap<String, String>,
    /// 创建时间（Unix 时间戳，秒）
    pub created_at: u64,
    /// 更新时间（Unix 时间戳，秒）
    pub updated_at: u64,
}

/// 图表数据
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChartData {
    /// 图表 ID
    pub chart_id: String,
    /// 数据系列
    pub series: Vec<DataSeries>,
}

/// 仪表盘数据
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DashboardData {
    /// 仪表盘 ID
    pub dashboard_id: String,
    /// 图表数据
    pub charts: Vec<ChartData>,
    /// 时间范围
    pub time_range: TimeRange,
    /// 开始时间（Unix 时间戳，毫秒）
    pub start_time: u64,
    /// 结束时间（Unix 时间戳，毫秒）
    pub end_time: u64,
}

/// 创建图表请求
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateChartRequest {
    /// 图表标题
    pub title: String,
    /// 图表描述
    pub description: Option<String>,
    /// 图表类型
    pub chart_type: ChartType,
    /// 指标查询
    pub metric_query: String,
    /// 刷新间隔
    pub refresh_interval: RefreshInterval,
    /// 是否显示图例
    pub show_legend: bool,
    /// 是否堆叠
    pub stacked: bool,
    /// 是否填充
    pub fill: bool,
    /// 是否显示点
    pub show_points: bool,
    /// 是否显示阈值
    pub show_thresholds: bool,
    /// 阈值
    pub thresholds: Vec<f64>,
    /// 阈值颜色
    pub threshold_colors: Vec<String>,
    /// 单位
    pub unit: Option<String>,
    /// 最小值
    pub min: Option<f64>,
    /// 最大值
    pub max: Option<f64>,
    /// 小数位数
    pub decimals: Option<u32>,
    /// 自定义配置
    pub custom_config: Option<HashMap<String, String>>,
}

/// 创建仪表盘请求
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateDashboardRequest {
    /// 仪表盘标题
    pub title: String,
    /// 仪表盘描述
    pub description: Option<String>,
    /// 是否为系统仪表盘
    pub is_system: bool,
    /// 所有者
    pub owner: Option<String>,
    /// 标签
    pub tags: Vec<String>,
    /// 时间范围
    pub time_range: TimeRange,
    /// 自定义开始时间（Unix 时间戳，毫秒）
    pub custom_start_time: Option<u64>,
    /// 自定义结束时间（Unix 时间戳，毫秒）
    pub custom_end_time: Option<u64>,
    /// 刷新间隔
    pub refresh_interval: RefreshInterval,
    /// 图表配置
    pub charts: Vec<CreateChartRequest>,
    /// 布局
    pub layout: Vec<DashboardLayoutItem>,
    /// 变量
    pub variables: HashMap<String, String>,
}

/// 更新仪表盘请求
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateDashboardRequest {
    /// 仪表盘标题
    pub title: Option<String>,
    /// 仪表盘描述
    pub description: Option<String>,
    /// 所有者
    pub owner: Option<String>,
    /// 标签
    pub tags: Option<Vec<String>>,
    /// 时间范围
    pub time_range: Option<TimeRange>,
    /// 自定义开始时间（Unix 时间戳，毫秒）
    pub custom_start_time: Option<u64>,
    /// 自定义结束时间（Unix 时间戳，毫秒）
    pub custom_end_time: Option<u64>,
    /// 刷新间隔
    pub refresh_interval: Option<RefreshInterval>,
    /// 图表配置
    pub charts: Option<Vec<ChartConfig>>,
    /// 布局
    pub layout: Option<Vec<DashboardLayoutItem>>,
    /// 变量
    pub variables: Option<HashMap<String, String>>,
}

/// 查询仪表盘请求
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryDashboardsRequest {
    /// 标题模式（支持部分匹配）
    pub title_pattern: Option<String>,
    /// 是否为系统仪表盘
    pub is_system: Option<bool>,
    /// 所有者
    pub owner: Option<String>,
    /// 标签
    pub tags: Option<Vec<String>>,
    /// 限制返回数量
    pub limit: Option<u32>,
    /// 偏移量（用于分页）
    pub offset: Option<u32>,
}

/// 查询仪表盘响应
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryDashboardsResponse {
    /// 仪表盘列表
    pub dashboards: Vec<DashboardConfig>,
    /// 总数
    pub total: u32,
}

/// 获取仪表盘数据请求
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetDashboardDataRequest {
    /// 仪表盘 ID
    pub dashboard_id: String,
    /// 时间范围
    pub time_range: Option<TimeRange>,
    /// 自定义开始时间（Unix 时间戳，毫秒）
    pub custom_start_time: Option<u64>,
    /// 自定义结束时间（Unix 时间戳，毫秒）
    pub custom_end_time: Option<u64>,
    /// 变量
    pub variables: Option<HashMap<String, String>>,
}

/// 仪表盘模板
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DashboardTemplate {
    /// 模板 ID
    pub id: String,
    /// 模板名称
    pub name: String,
    /// 模板描述
    pub description: Option<String>,
    /// 模板类型
    pub template_type: String,
    /// 仪表盘配置
    pub dashboard_config: DashboardConfig,
}
