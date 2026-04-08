# GitHub 提交与 Pages 发布清单

## 1. 初始化与检查

```bash
git init
git checkout -b main
git add .
git status
```

重点确认未被加入提交的大文件：

- `data/raw/**`
- `data/processed/**`
- `app/backend/gisdatamonitor.sqlite3`
- `dist/**`
- `build/**`

## 2. 提交

```bash
git commit -m "chore: prepare repository for github and pages deployment"
```

## 3. 关联远程并推送

```bash
git remote add origin <your-repo-url>
git push -u origin main
```

## 4. 启用 GitHub Pages

1. 打开仓库 `Settings -> Pages`
2. `Source` 选择 `GitHub Actions`
3. 可选配置仓库变量 `GISDATAMONITOR_API_BASE`
4. 等待 `Deploy GitHub Pages` 工作流完成

发布目录来自 `app/frontend`，无需额外构建步骤。
