# mongodb_ftdc_decoder

FTDC decoder

A minimal Go tool to visualize MongoDB FTDC metric deltas via a local web dashboard.

Quick Start
git clone https://github.com/yunusuyanik/mongodb_ftdc_decoder.git
cd mongodb_ftdc_decoder
go mod tidy
go run main.go -dir /path/to/diagnostic.data


Open the printed URL (e.g., http://127.0.0.1:generatedport/) in your browser.

Flags

-dir: Required. Path to diagnostic.data

-debug: Print verbose logs

Output Files

metric_deltas.log: Metric delta values

ftdc_utilization.log: Utilization summary

web/: Dashboard static files included

Project Layout
ftdc_dashboard/
├─ main.go
├─ go.mod / go.sum
├─ web/

Tips

Ensure -dir points to the folder containing metrics.* files.

Using -debug helps troubleshoot issues.


Bu kısa formatı GitHub README’ine kolayca kopyala-yapıştır yapabilirsin. İstersen bunu senin adına repo üzerinde uygulayıp pull request hazırlayabilirim — ya da başka eklemeler istersen söyle, örneğin sabit port seçeneği, CSV/JSON export gibi.
