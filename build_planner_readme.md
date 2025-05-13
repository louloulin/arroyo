# 构建 arroyo-planner 模块（不包含 arroyo-operator）

本项目提供了一个脚本，用于构建 `arroyo-planner` 模块，而不构建 `arroyo-operator` 模块。这对于只需要使用 `arroyo-planner` 功能的场景非常有用。

## 背景

在某些情况下，我们可能只需要使用 `arroyo-planner` 模块的功能，而不需要 `arroyo-operator` 模块。由于 `arroyo-operator` 模块可能存在一些编译错误或依赖问题，我们需要一种方法来单独构建 `arroyo-planner` 模块。

## 解决方案

我们提供了一个脚本 `build_planner_without_operator.sh`，它通过以下步骤实现了这一目标：

1. 临时修改 `arroyo-planner` 的 `Cargo.toml` 文件，移除 `arroyo-operator` 依赖
2. 使用修改后的 `Cargo.toml` 文件构建 `arroyo-planner` 模块
3. 构建完成后，恢复原始的 `Cargo.toml` 文件

## 使用方法

```bash
# 给脚本添加执行权限
chmod +x build_planner_without_operator.sh

# 运行脚本
./build_planner_without_operator.sh
```

## 注意事项

- 脚本会临时修改 `Cargo.toml` 文件，但会在构建完成后恢复原始文件
- 脚本使用了 `--no-default-features --features="planner,no-operator"` 参数来构建 `arroyo-planner` 模块
- 如果构建过程中遇到错误，脚本会尝试恢复原始的 `Cargo.toml` 文件

## 实现细节

脚本的主要实现逻辑如下：

1. 创建临时目录，用于备份原始的 `Cargo.toml` 文件
2. 修改 `Cargo.toml` 文件，移除 `arroyo-operator` 依赖
3. 使用 `cargo build` 命令构建 `arroyo-planner` 模块
4. 恢复原始的 `Cargo.toml` 文件
5. 清理临时目录

这种方法允许我们在不修改原始代码库的情况下，单独构建 `arroyo-planner` 模块。
