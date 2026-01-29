package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"time"

	yahoofinanceapi "github.com/oscarli916/yahoo-finance-api"
)

type PricePoint struct {
	Date  time.Time
	Close float64
}

type Series struct {
	Symbol string
	Points []PricePoint
}

type ReportRow struct {
	Date   string
	ETF    float64
	Index  float64
	Alpha  float64
	Life   float64
	Glide  float64
	Weight float64
}

func loadFromYahoo(symbol string, query yahoofinanceapi.HistoryQuery) (Series, error) {
	ticker := yahoofinanceapi.NewTicker(symbol)
	data, err := ticker.History(query)
	if err != nil {
		return Series{}, fmt.Errorf("history error %s: %w", symbol, err)
	}

	points := make([]PricePoint, 0, len(data))
	skipped := 0
	for dateStr, price := range data {
		if math.IsNaN(price.Close) || price.Close <= 0 {
			skipped++
			continue
		}

		d, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			d, err = time.Parse("2006-01-02 15:04:05", dateStr)
			if err != nil {
				return Series{}, fmt.Errorf("parse date %s for %s: %w", dateStr, symbol, err)
			}
		}

		points = append(points, PricePoint{
			Date:  d.UTC(),
			Close: price.Close,
		})
	}

	sort.Slice(points, func(i, j int) bool { return points[i].Date.Before(points[j].Date) })
	if skipped > 0 {
		fmt.Fprintf(os.Stderr, "Ticker %s: skipped %d NaN Close points\n", symbol, skipped)
	}

	return Series{Symbol: symbol, Points: points}, nil
}

// monthlySeries returns the last close of each month keyed by the first day of month (UTC).
func monthlySeries(points []PricePoint) map[time.Time]float64 {
	m := make(map[time.Time]float64)
	for _, p := range points {
		y, mon, _ := p.Date.Date()
		key := time.Date(y, mon, 1, 0, 0, 0, 0, time.UTC)
		m[key] = p.Close
	}
	return m
}

// monthlyReturns converts monthly prices into month-over-month returns.
func monthlyReturns(series map[time.Time]float64) ([]time.Time, []float64) {
	if len(series) == 0 {
		return nil, nil
	}

	dates := make([]time.Time, 0, len(series))
	for d := range series {
		dates = append(dates, d)
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })

	rets := make([]float64, 0, len(dates)-1)
	outDates := make([]time.Time, 0, len(dates)-1)
	for i := 1; i < len(dates); i++ {
		d0 := dates[i-1]
		d1 := dates[i]
		p0 := series[d0]
		p1 := series[d1]
		if p0 == 0 {
			continue
		}
		rets = append(rets, p1/p0-1.0)
		outDates = append(outDates, d1)
	}
	return outDates, rets
}

func alignReturns(datesA []time.Time, retsA []float64, datesB []time.Time, retsB []float64) ([]time.Time, []float64, []float64) {
	index := make(map[time.Time]float64, len(datesB))
	for i, d := range datesB {
		index[d] = retsB[i]
	}

	alignedDates := make([]time.Time, 0, len(datesA))
	alignedA := make([]float64, 0, len(datesA))
	alignedB := make([]float64, 0, len(datesA))
	for i, d := range datesA {
		if r, ok := index[d]; ok {
			alignedDates = append(alignedDates, d)
			alignedA = append(alignedA, retsA[i])
			alignedB = append(alignedB, r)
		}
	}
	return alignedDates, alignedA, alignedB
}

func cumulative(base float64, returns []float64) []float64 {
	res := make([]float64, len(returns))
	v := base
	for i, r := range returns {
		v *= 1 + r
		res[i] = v
	}
	return res
}

func blendReturns(retsA []float64, retsB []float64, weightA float64) []float64 {
	out := make([]float64, len(retsA))
	weightB := 1 - weightA
	for i := range retsA {
		out[i] = retsA[i]*weightA + retsB[i]*weightB
	}
	return out
}

func glideWeights(count int, start float64, end float64) []float64 {
	if count <= 1 {
		return []float64{end}
	}
	weights := make([]float64, count)
	step := (end - start) / float64(count-1)
	for i := 0; i < count; i++ {
		weights[i] = start + float64(i)*step
	}
	return weights
}

func validateWeight(name string, v float64) error {
	if v < 0 || v > 1 {
		return fmt.Errorf("%s must be between 0 and 1", name)
	}
	return nil
}

func parseDate(value string) error {
	_, err := time.Parse("2006-01-02", value)
	return err
}

func writeHTMLReport(path string, etfSymbol string, idxSymbol string, startDate string, interval string, lifeWeight float64, glideStart float64, glideEnd float64, rows []ReportRow, avgAlpha float64, winCount int, total int) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve html path: %w", err)
	}
	f, err := os.Create(absPath)
	if err != nil {
		return "", fmt.Errorf("create html: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	w := bufio.NewWriter(f)
	defer func() {
		_ = w.Flush()
	}()

	_, _ = w.WriteString("<!doctype html>\n<html lang=\"it\">\n<head>\n<meta charset=\"utf-8\">\n")
	_, _ = w.WriteString("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n")
	_, _ = w.WriteString("<title>ETF vs Index Report</title>\n")
	_, _ = w.WriteString("<script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>\n")
	_, _ = w.WriteString("<style>\n")
	_, _ = w.WriteString("body{font-family:Arial,Helvetica,sans-serif;background:#f6f7fb;color:#1b1b1b;margin:0;padding:24px}\n")
	_, _ = w.WriteString(".wrap{max-width:1200px;margin:0 auto}\n")
	_, _ = w.WriteString("h1{margin:0 0 8px 0}\n")
	_, _ = w.WriteString(".meta{color:#555;margin-bottom:16px}\n")
	_, _ = w.WriteString(".cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin:16px 0 24px 0}\n")
	_, _ = w.WriteString(".card{background:#fff;border-radius:10px;padding:14px;border:1px solid #e3e5ee}\n")
	_, _ = w.WriteString(".card .label{color:#666;font-size:12px;text-transform:uppercase}\n")
	_, _ = w.WriteString(".card .value{font-size:20px;font-weight:700;margin-top:6px}\n")
	_, _ = w.WriteString("canvas{background:#fff;border-radius:10px;border:1px solid #e3e5ee;padding:12px}\n")
	_, _ = w.WriteString("table{width:100%;border-collapse:collapse;background:#fff;border-radius:10px;overflow:hidden;border:1px solid #e3e5ee;margin-top:20px}\n")
	_, _ = w.WriteString("th,td{padding:8px 10px;border-bottom:1px solid #eef0f5;text-align:right;font-size:13px}\n")
	_, _ = w.WriteString("th:first-child,td:first-child{text-align:left}\n")
	_, _ = w.WriteString("thead{background:#f0f3fb}\n")
	_, _ = w.WriteString("</style>\n</head>\n<body>\n<div class=\"wrap\">\n")
	_, _ = fmt.Fprintf(w, "<h1>ETF vs Index</h1>\n")
	_, _ = fmt.Fprintf(w, "<div class=\"meta\">ETF: %s | Index: %s | Start: %s | Interval: %s</div>\n", etfSymbol, idxSymbol, startDate, interval)
	_, _ = w.WriteString("<div class=\"cards\">\n")
	_, _ = fmt.Fprintf(w, "<div class=\"card\"><div class=\"label\">Win rate</div><div class=\"value\">%d/%d</div></div>\n", winCount, total)
	_, _ = fmt.Fprintf(w, "<div class=\"card\"><div class=\"label\">Avg alpha</div><div class=\"value\">%.5f</div></div>\n", avgAlpha)
	_, _ = fmt.Fprintf(w, "<div class=\"card\"><div class=\"label\">Life ETF weight</div><div class=\"value\">%.2f</div></div>\n", lifeWeight)
	_, _ = fmt.Fprintf(w, "<div class=\"card\"><div class=\"label\">Glide start/end</div><div class=\"value\">%.2f → %.2f</div></div>\n", glideStart, glideEnd)
	_, _ = w.WriteString("</div>\n")
	_, _ = w.WriteString("<canvas id=\"cumChart\" height=\"120\"></canvas>\n")
	_, _ = w.WriteString("<div style=\"height:16px\"></div>\n")
	_, _ = w.WriteString("<canvas id=\"alphaChart\" height=\"90\"></canvas>\n")

	_, _ = w.WriteString("<table>\n<thead><tr>")
	_, _ = w.WriteString("<th>Date</th><th>ETF</th><th>Index</th><th>Alpha</th><th>LifeStrategy</th><th>GlidePath</th><th>GlideETF</th>")
	_, _ = w.WriteString("</tr></thead>\n<tbody>\n")
	for _, r := range rows {
		_, _ = fmt.Fprintf(w, "<tr><td>%s</td><td>%.2f</td><td>%.2f</td><td>%.5f</td><td>%.2f</td><td>%.2f</td><td>%.4f</td></tr>\n",
			r.Date, r.ETF, r.Index, r.Alpha, r.Life, r.Glide, r.Weight)
	}
	_, _ = w.WriteString("</tbody>\n</table>\n")

	_, _ = w.WriteString("<script>\n")
	_, _ = w.WriteString("const labels = [")
	for i, r := range rows {
		if i > 0 {
			_, _ = w.WriteString(",")
		}
		_, _ = fmt.Fprintf(w, "\"%s\"", r.Date)
	}
	_, _ = w.WriteString("];\n")

	_, _ = w.WriteString("const etfData = [")
	for i, r := range rows {
		if i > 0 {
			_, _ = w.WriteString(",")
		}
		_, _ = fmt.Fprintf(w, "%.2f", r.ETF)
	}
	_, _ = w.WriteString("];\n")

	_, _ = w.WriteString("const indexData = [")
	for i, r := range rows {
		if i > 0 {
			_, _ = w.WriteString(",")
		}
		_, _ = fmt.Fprintf(w, "%.2f", r.Index)
	}
	_, _ = w.WriteString("];\n")

	_, _ = w.WriteString("const lifeData = [")
	for i, r := range rows {
		if i > 0 {
			_, _ = w.WriteString(",")
		}
		_, _ = fmt.Fprintf(w, "%.2f", r.Life)
	}
	_, _ = w.WriteString("];\n")

	_, _ = w.WriteString("const glideData = [")
	for i, r := range rows {
		if i > 0 {
			_, _ = w.WriteString(",")
		}
		_, _ = fmt.Fprintf(w, "%.2f", r.Glide)
	}
	_, _ = w.WriteString("];\n")

	_, _ = w.WriteString("const alphaData = [")
	for i, r := range rows {
		if i > 0 {
			_, _ = w.WriteString(",")
		}
		_, _ = fmt.Fprintf(w, "%.5f", r.Alpha)
	}
	_, _ = w.WriteString("];\n")

	_, _ = w.WriteString("new Chart(document.getElementById('cumChart'),{type:'line',data:{labels:labels,datasets:[")
	_, _ = w.WriteString("{label:'ETF',data:etfData,borderColor:'#1f77b4',backgroundColor:'rgba(31,119,180,0.1)',tension:0.2},")
	_, _ = w.WriteString("{label:'Index',data:indexData,borderColor:'#ff7f0e',backgroundColor:'rgba(255,127,14,0.1)',tension:0.2},")
	_, _ = w.WriteString("{label:'LifeStrategy',data:lifeData,borderColor:'#2ca02c',backgroundColor:'rgba(44,160,44,0.1)',tension:0.2},")
	_, _ = w.WriteString("{label:'GlidePath',data:glideData,borderColor:'#9467bd',backgroundColor:'rgba(148,103,189,0.1)',tension:0.2}")
	_, _ = w.WriteString("]},options:{plugins:{legend:{position:'bottom'}},scales:{y:{title:{display:true,text:'Cumulative (base 100)'}}}}});\n")
	_, _ = w.WriteString("new Chart(document.getElementById('alphaChart'),{type:'bar',data:{labels:labels,datasets:[{label:'Alpha',data:alphaData,backgroundColor:'rgba(220,53,69,0.35)',borderColor:'#dc3545'}]},")
	_, _ = w.WriteString("options:{plugins:{legend:{position:'bottom'}},scales:{y:{title:{display:true,text:'Monthly alpha'}}}}});\n")
	_, _ = w.WriteString("</script>\n")
	_, _ = w.WriteString("</div>\n</body>\n</html>\n")

	return absPath, nil
}

func main() {
	var (
		etfSymbol  string
		idxSymbol  string
		startDate  string
		interval   string
		outPath    string
		htmlPath   string
		lifeWeight float64
		glideStart float64
		glideEnd   float64
		verify     bool
	)

	flag.StringVar(&etfSymbol, "etf", "SPY", "ETF symbol")
	flag.StringVar(&idxSymbol, "index", "^990100-USD-STRD", "Reference index symbol")
	flag.StringVar(&startDate, "start", "2019-01-01", "Start date (YYYY-MM-DD)")
	flag.StringVar(&interval, "interval", "1d", "Yahoo interval")
	flag.StringVar(&outPath, "out", "", "Output CSV path (empty for stdout)")
	flag.StringVar(&htmlPath, "html", "", "Output HTML report path (empty to skip)")
	flag.Float64Var(&lifeWeight, "life-etf", 0.80, "LifeStrategy ETF weight")
	flag.Float64Var(&glideStart, "glide-start", 0.90, "Glide path start ETF weight")
	flag.Float64Var(&glideEnd, "glide-end", 0.60, "Glide path end ETF weight")
	flag.BoolVar(&verify, "verify", false, "Print sample verification rows to stderr")
	flag.Parse()

	if err := parseDate(startDate); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid start date %q: %v\n", startDate, err)
		os.Exit(1)
	}
	if err := validateWeight("life-etf", lifeWeight); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := validateWeight("glide-start", glideStart); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := validateWeight("glide-end", glideEnd); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	query := yahoofinanceapi.HistoryQuery{
		Start:    startDate,
		Interval: interval,
	}

	etfSeries, err := loadFromYahoo(etfSymbol, query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ETF error: %v\n", err)
		os.Exit(1)
	}
	idxSeries, err := loadFromYahoo(idxSymbol, query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Index error: %v\n", err)
		os.Exit(1)
	}

	etfMonthly := monthlySeries(etfSeries.Points)
	idxMonthly := monthlySeries(idxSeries.Points)

	datesE, retsE := monthlyReturns(etfMonthly)
	datesI, retsI := monthlyReturns(idxMonthly)

	alignedDates, alignedE, alignedI := alignReturns(datesE, retsE, datesI, retsI)
	if len(alignedDates) == 0 {
		fmt.Fprintln(os.Stderr, "No aligned months. Check symbols or date range.")
		os.Exit(1)
	}

	if verify {
		fmt.Fprintln(os.Stderr, "VERIFY sample rows (monthly closes and returns):")
		fmt.Fprintln(os.Stderr, "Date,ETF_Close,Index_Close,ETF_Return,Index_Return,Alpha")
		printSample := func(i int) {
			d := alignedDates[i]
			etfClose := etfMonthly[d]
			idxClose := idxMonthly[d]
			alpha := alignedE[i] - alignedI[i]
			fmt.Fprintf(os.Stderr, "%s,%.2f,%.2f,%.5f,%.5f,%.5f\n",
				d.Format("2006-01"),
				etfClose,
				idxClose,
				alignedE[i],
				alignedI[i],
				alpha,
			)
		}

		limit := 3
		if len(alignedDates) < limit {
			limit = len(alignedDates)
		}
		for i := 0; i < limit; i++ {
			printSample(i)
		}
		if len(alignedDates) > limit {
			fmt.Fprintln(os.Stderr, "...")
			for i := len(alignedDates) - limit; i < len(alignedDates); i++ {
				printSample(i)
			}
		}
	}

	lifeRets := blendReturns(alignedE, alignedI, lifeWeight)
	glideWeights := glideWeights(len(alignedDates), glideStart, glideEnd)
	glideRets := make([]float64, len(alignedDates))
	for i := range alignedDates {
		glideRets[i] = alignedE[i]*glideWeights[i] + alignedI[i]*(1-glideWeights[i])
	}

	cumE := cumulative(100, alignedE)
	cumI := cumulative(100, alignedI)
	cumLife := cumulative(100, lifeRets)
	cumGlide := cumulative(100, glideRets)

	var out *os.File
	if outPath != "" {
		f, err := os.Create(outPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Cannot create output file: %v\n", err)
			os.Exit(1)
		}
		defer func() {
			if cerr := f.Close(); cerr != nil {
				fmt.Fprintf(os.Stderr, "Failed to close output file: %v\n", cerr)
			}
		}()
		out = f
	} else {
		out = os.Stdout
	}

	writer := bufio.NewWriter(out)
	defer func() {
		if err := writer.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to flush output: %v\n", err)
		}
	}()

	_, _ = writer.WriteString("Date,ETF,Index,Alpha,LifeStrategy,GlidePath,GlideEtfWeight\n")

	validCount := 0
	winCount := 0
	sumAlpha := 0.0
	rows := make([]ReportRow, 0, len(alignedDates))
	for i, d := range alignedDates {
		alpha := alignedE[i] - alignedI[i]
		if math.IsNaN(alpha) || math.IsInf(alpha, 0) {
			continue
		}
		validCount++
		if alpha > 0 {
			winCount++
		}
		sumAlpha += alpha

		_, _ = fmt.Fprintf(writer, "%s,%.2f,%.2f,%.5f,%.2f,%.2f,%.4f\n",
			d.Format("2006-01"),
			cumE[i],
			cumI[i],
			alpha,
			cumLife[i],
			cumGlide[i],
			glideWeights[i],
		)

		rows = append(rows, ReportRow{
			Date:   d.Format("2006-01"),
			ETF:    cumE[i],
			Index:  cumI[i],
			Alpha:  alpha,
			Life:   cumLife[i],
			Glide:  cumGlide[i],
			Weight: glideWeights[i],
		})
	}

	if validCount == 0 {
		fmt.Fprintln(os.Stderr, "No valid months for ETF vs index comparison.")
		os.Exit(1)
	}

	avgAlpha := sumAlpha / float64(validCount)
	fmt.Fprintf(os.Stderr, "Tracking difference: ETF>index=%d/%d, avg=%.5f\n", winCount, validCount, avgAlpha)

	lastE := cumE[len(cumE)-1]
	lastI := cumI[len(cumI)-1]
	if math.IsNaN(lastE) || math.IsNaN(lastI) {
		fmt.Fprintln(os.Stderr, "Final comparison not available: insufficient data.")
		os.Exit(1)
	}
	result := "equal to"
	if lastE > lastI {
		result = "higher than"
	} else if lastE < lastI {
		result = "lower than"
	}
	fmt.Fprintf(os.Stderr, "Result: %s is %s index (%.2f vs %.2f)\n", etfSymbol, result, lastE, lastI)

	if htmlPath != "" {
		reportPath, err := writeHTMLReport(htmlPath, etfSymbol, idxSymbol, startDate, interval, lifeWeight, glideStart, glideEnd, rows, avgAlpha, winCount, validCount)
		if err != nil {
			fmt.Fprintf(os.Stderr, "HTML report error: %v\n", err)
			os.Exit(1)
		}

		cmd := exec.Command("cmd", "/c", "start", "", reportPath)
		if err := cmd.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open report: %v\n", err)
		}
	}
}
