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
| `-genlogs` | Generate decoded log and parsed log files with deltas                   |

---

## 📄 Output Files

- **`metric_deltas_{timestamp}.log`** – Parsed metric delta values
- **`fully_decoded_metrics_{timestamp}.log`** – Decoded logs

