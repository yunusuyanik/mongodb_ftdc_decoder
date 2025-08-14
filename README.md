# mongodb_ftdc_decoder
**FTDC decoder** – A minimal Go tool to visualize MongoDB FTDC metric deltas via a local web dashboard.

---

## 🚀 Quick Start

```bash
git clone https://github.com/yunusuyanik/mongodb_ftdc_decoder.git
cd mongodb_ftdc_decoder
go mod tidy
go run main.go -dir /path/to/diagnostic.data
```

Open the printed URL (e.g., `http://127.0.0.1:<generatedport>/`) in your browser.

---

## ⚙️ Flags

| Flag     | Description                          |
|----------|--------------------------------------|
| `-dir`   | **Required.** Path to `diagnostic.data` |
| `-debug` | Print verbose logs                   |

---

## 📄 Output Files

- **`metric_deltas.log`** – Metric delta values
- **`ftdc_utilization.log`** – Utilization summary

---

## 📂 Project Layout
```
ftdc_dashboard/
├─ main.go
├─ go.mod
├─ go.sum
├─ web/
└─ web/index.html
```

---

## 💡 Tips

- Ensure `-dir` points to the folder containing `metrics.*` files.
- Use `-debug` to get detailed logs for troubleshooting.
