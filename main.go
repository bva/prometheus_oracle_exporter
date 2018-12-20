package main

import (
    "fmt"
    "database/sql"
    "flag"
    "net/http"
    "time"
    "io/ioutil"
    "gopkg.in/yaml.v2"
  _ "gopkg.in/rana/ora.v4"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "github.com/prometheus/common/log"
)

// Metric name parts.
const (
  namespace = "oracledb"
  exporter  = "exporter"
)

// Exporter collects Oracle DB metrics. It implements prometheus.Collector.
type Exporter struct {
  duration, error *prometheus.GaugeVec
  totalScrapes    *prometheus.CounterVec
  scrapeErrors    *prometheus.CounterVec
  session         *prometheus.GaugeVec
  sysstat         *prometheus.GaugeVec
  waitclass       *prometheus.GaugeVec
  sysmetric       *prometheus.GaugeVec
  interconnect    *prometheus.GaugeVec
  uptime          *prometheus.GaugeVec
  up              *prometheus.GaugeVec
  tablespace      *prometheus.GaugeVec
  recovery        *prometheus.GaugeVec
  redo            *prometheus.GaugeVec
  cache           *prometheus.GaugeVec
  services        *prometheus.GaugeVec
  parameter       *prometheus.GaugeVec
  query           *prometheus.GaugeVec
  asmspace        *prometheus.GaugeVec
  config          Config
}

var (
  // Version will be set at build time.
  Version       = "1.1.0"
  listenAddress = flag.String("web.listen-address", ":9161", "Address to listen on for web interface and telemetry.")
  metricPath    = flag.String("web.telemetry-path", "/scrape", "Path under which to expose metrics.")
  configFile    = flag.String("configfile", "oracle.yml", "ConfigurationFile in YAML format.")
  landingPage   = []byte(`<html>
                          <head><title>Prometheus Oracle exporter</title></head>
                          <body>
                            <h1>Prometheus Oracle exporter</h1><p>
                            <a href='` + *metricPath + `'>Scrape</a></p>
                          </body>
                          </html>`)

  configs Configs
  metricsExporter *Exporter
  handlers = map[string]http.Handler {}
)

// NewExporter returns a new Oracle DB exporter for the provided DSN.
func NewExporter() *Exporter {
  return &Exporter{
    duration: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Subsystem: exporter,
      Name:      "last_scrape_duration_seconds",
      Help:      "Duration of the last scrape of metrics from Oracle DB.",
    }, []string{"database","dbinstance"}),
    totalScrapes: prometheus.NewCounterVec(prometheus.CounterOpts{
      Namespace: namespace,
      Subsystem: exporter,
      Name:      "scrapes_total",
      Help:      "Total number of times Oracle DB was scraped for metrics.",
    }, []string{"database","dbinstance"}),
    scrapeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
      Namespace: namespace,
      Subsystem: exporter,
      Name:      "scrape_errors_total",
      Help:      "Total number of times an error occured scraping a Oracle database.",
    }, []string{"database","dbinstance"}),
    error: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Subsystem: exporter,
      Name:      "last_scrape_error",
      Help:      "Whether the last scrape of metrics from Oracle DB resulted in an error (1 for error, 0 for success).",
    },[]string{"database","dbinstance"}),
    sysmetric: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Name:      "sysmetric",
      Help:      "Gauge metric with read/write pysical IOPs/bytes (v$sysmetric).",
    }, []string{"database","dbinstance","type"}),
    waitclass: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Name:      "waitclass",
      Help:      "Gauge metric with Waitevents (v$waitclassmetric).",
    }, []string{"database","dbinstance","type"}),
    sysstat: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Name:      "sysstat",
      Help:      "Gauge metric with commits/rollbacks/parses (v$sysstat).",
    }, []string{"database","dbinstance","type"}),
    session: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Name:      "session",
      Help:      "Gauge metric user/system active/passive sessions (v$session).",
    }, []string{"database","dbinstance","type","state"}),
    uptime: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Name:      "uptime",
      Help:      "Gauge metric with uptime in days of the Instance.",
    }, []string{"database","dbinstance"}),
    tablespace: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Name:      "tablespace",
      Help:      "Gauge metric with total/free size of the Tablespaces.",
    }, []string{"database","dbinstance","type","name","contents","autoextend"}),
    interconnect: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Name:      "interconnect",
      Help:      "Gauge metric with interconnect block transfers (v$sysstat).",
    }, []string{"database","dbinstance","type"}),
    recovery: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Name:      "recovery",
      Help:      "Gauge metric with percentage usage of FRA (v$recovery_file_dest).",
    }, []string{"database","dbinstance","type"}),
    redo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Name:      "redo",
      Help:      "Gauge metric with Redo log switches over last 5 min (v$log_history).",
    }, []string{"database","dbinstance"}),
    cache: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Name:      "cachehitratio",
      Help:      "Gauge metric witch Cache hit ratios (v$sysmetric).",
    }, []string{"database","dbinstance","type"}),
    up: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Name:      "up",
      Help:      "Whether the Oracle server is up.",
    }, []string{"database","dbinstance"}),
    services: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Name:      "services",
      Help:      "Active Oracle Services (v$active_services).",
    }, []string{"database","dbinstance","name"}),
    parameter: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Name:      "parameter",
      Help:      "oracle Configuration Parameters (v$parameter).",
    }, []string{"database","dbinstance","name"}),
    query: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Name:      "query",
      Help:      "Self defined Queries from Configuration File.",
    }, []string{"database","dbinstance","name"}),
    asmspace: prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Name:      "asmspace",
      Help:      "Gauge metric with total/free size of the ASM Diskgroups.",
    }, []string{"database","dbinstance","type","name"}),
  }
}

// ScrapeQuery collects metrics from self defined queries from configuration file.
func (e *Exporter) ScrapeQuery() {
  var (
    rows *sql.Rows
    err  error
  )

  db := e.config.db
  //num  metric_name
  //43  sessions
  if db != nil {
    for _, query := range e.config.Queries {
      rows, err = db.Query(query.Sql)
      if err != nil {
        break
      }
      defer rows.Close()
      for rows.Next() {
        var value float64
        if err := rows.Scan(&value); err != nil {
          break
        }
        e.query.WithLabelValues(e.config.Database,e.config.Instance,query.Name).Set(value)
      }
    }
  }
}

// ScrapeParameters collects metrics from the v$parameters view.
func (e *Exporter) ScrapeParameter() {
  var (
    rows *sql.Rows
    err  error
  )

  db := e.config.db

  //num  metric_name
  //43  sessions
  if db != nil {
    rows, err = db.Query(`select name,value from v$parameter WHERE num=43`)
    if err != nil {
      return
    }

    defer rows.Close()

    for rows.Next() {
      var name string
      var value float64
      if err := rows.Scan(&name,&value); err != nil {
        break
      }
      name = cleanName(name)
      e.parameter.WithLabelValues(e.config.Database,e.config.Instance,name).Set(value)
    }
  }
}


// ScrapeServices collects metrics from the v$active_services view.
func (e *Exporter) ScrapeServices() {
  var (
    rows *sql.Rows
    err  error
  )

  config := e.config
  db := config.db

  if db != nil {
    rows, err = db.Query(`select name from v$active_services`)
    if err != nil {
      return
    }
    defer rows.Close()
    for rows.Next() {
      var name string
      if err := rows.Scan(&name); err != nil {
        break
      }
      name = cleanName(name)
      e.services.WithLabelValues(config.Database,config.Instance,name).Set(1)
    }
  }
}


// ScrapeCache collects session metrics from the v$sysmetrics view.
func (e *Exporter) ScrapeCache() {
  var (
    rows *sql.Rows
    err  error
  )

  config := e.config
  db := config.db

  //metric_id  metric_name
  //2000    Buffer Cache Hit Ratio
  //2050    Cursor Cache Hit Ratio
  //2112    Library Cache Hit Ratio
  //2110    Row Cache Hit Ratio

  if db != nil {
    rows, err = db.Query(`select metric_name,value
                               from v$sysmetric
                               where group_id=2 and metric_id in (2000,2050,2112,2110)`)
    if err != nil {
      return
    }
    defer rows.Close()
    for rows.Next() {
      var name string
      var value float64
      if err := rows.Scan(&name, &value); err != nil {
        break
      }
      name = cleanName(name)
      e.cache.WithLabelValues(config.Database,config.Instance,name).Set(value)
    }
  }
}


// ScrapeRecovery collects tablespace metrics
func (e *Exporter) ScrapeRedo() {
  var (
    rows *sql.Rows
    err  error
  )

  config := e.config
  db := config.db

  if db != nil {
    rows, err = db.Query(`select count(*) from v$log_history where first_time > sysdate - 1/24/12`)
    if err != nil {
      return
    }
    defer rows.Close()
    for rows.Next() {
      var value float64
      if err := rows.Scan(&value); err != nil {
        break
      }
      e.redo.WithLabelValues(config.Database,config.Instance).Set(value)
    }
  }
}

// ScrapeRecovery collects tablespace metrics
func (e *Exporter) ScrapeRecovery() {
  var (
    rows *sql.Rows
    err  error
  )

  config := e.config
  db := config.db

  if db != nil {
    rows, err = db.Query(`SELECT sum(percent_space_used) , sum(percent_space_reclaimable)
                             from V$FLASH_RECOVERY_AREA_USAGE`)
    if err != nil {
      return
    }
    defer rows.Close()
    for rows.Next() {
      var used float64
      var recl float64
      if err := rows.Scan(&used, &recl); err != nil {
        break
      }
      e.recovery.WithLabelValues(config.Database,config.Instance,"percent_space_used").Set(used)
      e.recovery.WithLabelValues(config.Database,config.Instance,"percent_space_reclaimable").Set(recl)
    }
  }
}

// ScrapeTablespaces collects tablespace metrics
func (e *Exporter) ScrapeInterconnect() {
  var (
    rows *sql.Rows
    err  error
  )

  config := e.config
  db := config.db

  if db != nil {
    rows, err = db.Query(`SELECT name, value
                               FROM V$SYSSTAT
                               WHERE name in ('gc cr blocks served','gc cr blocks flushed','gc cr blocks received')`)
    if err != nil {
      return
    }
    defer rows.Close()
    for rows.Next() {
      var name string
      var value float64
      if err := rows.Scan(&name, &value); err != nil {
        break
      }
      name = cleanName(name)
      e.interconnect.WithLabelValues(config.Database,config.Instance,name).Set(value)
    }
  }
}

// ScrapeAsmspace collects ASM metrics
func (e *Exporter) ScrapeAsmspace() {
  var (
    rows *sql.Rows
    err  error
  )

  config := e.config
  db := config.db

  if db != nil {
    rows, err = db.Query(`SELECT g.name, sum(d.total_mb), sum(d.free_mb)
                                FROM v$asm_disk d, v$asm_diskgroup g
                               WHERE  d.group_number = g.group_number
                                AND  d.header_status = 'MEMBER'
                               GROUP by  g.name,  g.group_number`)
    if err != nil {
      return
    }
    defer rows.Close()
    for rows.Next() {
      var name string
      var tsize float64
      var tfree float64
      if err := rows.Scan(&name, &tsize, &tfree); err != nil {
        break
      }
      e.asmspace.WithLabelValues(config.Database,config.Instance,"total",name).Set(tsize)
      e.asmspace.WithLabelValues(config.Database,config.Instance,"free",name).Set(tfree)
      e.asmspace.WithLabelValues(config.Database,config.Instance,"used",name).Set(tsize-tfree)
    }
  }
}


// ScrapeTablespaces collects tablespace metrics
func (e *Exporter) ScrapeTablespace() {
  var (
    rows *sql.Rows
    err  error
  )

  config := e.config
  db := config.db

  if db != nil {
    rows, err = db.Query(`WITH
                                 getsize AS (SELECT tablespace_name, autoextensible, SUM(bytes) tsize
                                             FROM dba_data_files GROUP BY tablespace_name, autoextensible),
                                 getfree as (SELECT tablespace_name, contents, SUM(blocks*block_size) tfree
                                             FROM DBA_LMT_FREE_SPACE a, v$tablespace b, dba_tablespaces c
                                             WHERE a.TABLESPACE_ID= b.ts# and b.name=c.tablespace_name
                                             GROUP BY tablespace_name,contents)
                               SELECT a.tablespace_name, b.contents, a.tsize,  b.tfree, a.autoextensible autoextend
                               FROM GETSIZE a, GETFREE b
                               WHERE a.tablespace_name = b.tablespace_name
                               UNION
                               SELECT tablespace_name, 'TEMPORARY', sum(tablespace_size), sum(free_space), 'NO'
                               FROM dba_temp_free_space
                               GROUP BY tablespace_name`)
    if err != nil {
      return
    }
    defer rows.Close()
    for rows.Next() {
      var name string
      var contents string
      var tsize float64
      var tfree float64
      var auto string
      if err := rows.Scan(&name, &contents, &tsize, &tfree, &auto); err != nil {
        break
      }
      e.tablespace.WithLabelValues(config.Database,config.Instance,"total",name,contents,auto).Set(tsize)
      e.tablespace.WithLabelValues(config.Database,config.Instance,"free",name,contents,auto).Set(tfree)
      e.tablespace.WithLabelValues(config.Database,config.Instance,"used",name,contents,auto).Set(tsize-tfree)
    }
  }
}

// ScrapeSessions collects session metrics from the v$session view.
func (e *Exporter) ScrapeSession() {
  var (
    rows *sql.Rows
    err  error
  )

  config := e.config
  db := config.db

  if db != nil {
    rows, err = db.Query(`SELECT decode(username,NULL,'SYSTEM','SYS','SYSTEM','USER'), status,count(*)
                               FROM v$session
                               GROUP BY decode(username,NULL,'SYSTEM','SYS','SYSTEM','USER'),status`)
    if err != nil {
      return
    }
    defer rows.Close()
    for rows.Next() {
      var user string
      var status string
      var value float64
      if err := rows.Scan(&user, &status, &value); err != nil {
        break
      }
      e.session.WithLabelValues(config.Database,config.Instance,user,status).Set(value)
    }
  }
}


// ScrapeUptime Instance uptime
func (e *Exporter) ScrapeUptime() {
  var uptime float64

  config := e.config
  db := config.db

  if db != nil {
    rows, err := db.Query("select sysdate-startup_time from v$instance")
    if err != nil {
      return
    }

    defer rows.Close()
    rows.Next()
    err = rows.Scan(&uptime)
    if err == nil {
      e.uptime.WithLabelValues(config.Database,config.Instance).Set(uptime)
    }
  }
}

// ScrapeSysstat collects activity metrics from the v$sysstat view.
func (e *Exporter) ScrapeSysstat() {
  var (
    rows *sql.Rows
    err  error
  )

  config := e.config
  db := config.db

  if db != nil {
    rows, err = db.Query(`SELECT name, value FROM v$sysstat
                                    WHERE statistic# in (6,7,1084,1089)`)
    if err != nil {
      return
    }
    defer rows.Close()
    for rows.Next() {
      var name string
      var value float64
      if err := rows.Scan(&name, &value); err != nil {
        break
      }
      name = cleanName(name)
      e.sysstat.WithLabelValues(config.Database,config.Instance,name).Set(value)
    }
  }
}

// ScrapeWaitTime collects wait time metrics from the v$waitclassmetric view.
func (e *Exporter) ScrapeWaitclass() {
  var (
    rows *sql.Rows
    err  error
  )

  config := e.config
  db := config.db

  if db != nil {
    rows, err = db.Query(`SELECT n.wait_class, round(m.time_waited/m.INTSIZE_CSEC,3)
                                  FROM v$waitclassmetric  m, v$system_wait_class n
                                  WHERE m.wait_class_id=n.wait_class_id and n.wait_class != 'Idle'`)
    if err != nil {
      return
    }
    defer rows.Close()
    for rows.Next() {
      var name string
      var value float64
      if err := rows.Scan(&name, &value); err != nil {
        break
      }
      name = cleanName(name)
      e.waitclass.WithLabelValues(config.Database,config.Instance,name).Set(value)
    }
  }
}

// ScrapeSysmetrics collects session metrics from the v$sysmetrics view.
func (e *Exporter) ScrapeSysmetric() {
  var (
    rows *sql.Rows
    err  error
  )

  config := e.config
  db := config.db

  //metric_id  metric_name
  //2092    Physical Read Total IO Requests Per Sec
  //2093    Physical Read Total Bytes Per Sec
  //2100    Physical Write Total IO Requests Per Sec
  //2124    Physical Write Total Bytes Per Sec
  if db != nil {
    rows, err = db.Query("select metric_name,value from v$sysmetric where metric_id in (2092,2093,2124,2100)")
    if err != nil {
      return
    }
    defer rows.Close()
    for rows.Next() {
      var name string
      var value float64
      if err := rows.Scan(&name, &value); err != nil {
        break
      }
      name = cleanName(name)
      e.sysmetric.WithLabelValues(config.Database,config.Instance,name).Set(value)
    }
  }
}

// Describe describes all the metrics exported by the Oracle exporter.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
  e.up.Describe(ch)
  e.session.Describe(ch)
  e.sysstat.Describe(ch)
  e.duration.Describe(ch)
  e.totalScrapes.Describe(ch)
  e.scrapeErrors.Describe(ch)
  e.waitclass.Describe(ch)
  e.sysmetric.Describe(ch)
  e.interconnect.Describe(ch)
  e.tablespace.Describe(ch)
  e.recovery.Describe(ch)
  e.redo.Describe(ch)
  e.cache.Describe(ch)
  e.uptime.Describe(ch)
  e.services.Describe(ch)
  e.parameter.Describe(ch)
  e.query.Describe(ch)
  e.asmspace.Describe(ch)
}

// Connect the DBs and gather Databasename and Instancename
func (e *Exporter) Connect() {
  e.up.Reset()
  e.session.Reset()
  e.sysstat.Reset()
  e.waitclass.Reset()
  e.sysmetric.Reset()
  e.interconnect.Reset()
  e.tablespace.Reset()
  e.recovery.Reset()
  e.redo.Reset()
  e.cache.Reset()
  e.uptime.Reset()
  e.services.Reset()
  e.parameter.Reset()
  e.query.Reset()
  e.asmspace.Reset()

  config := &e.config

  dsn := fmt.Sprintf("%s/%s@%s", config.User, config.Password, config.Connection)
  db , err := sql.Open("ora", dsn)
  config.db = db

  if err != nil {
    log.Infoln(err)
    e.up.WithLabelValues(config.Database,config.Instance).Set(0)

    if db != nil {
      db.Close()
      config.db = nil
    }

    return
  }

  rows, err := db.Query("select db_unique_name,instance_name from v$database,v$instance")
  if err != nil {
    log.Infoln(err)
    db.Close()
    config.db = nil

    e.up.WithLabelValues(config.Database,config.Instance).Set(0)
    return
  }

  defer rows.Close()
  rows.Next()
  err = rows.Scan(&config.Database,&config.Instance)

  if err == nil {
    e.up.WithLabelValues(config.Database, config.Instance).Set(1)
  } else {
    db.Close()
    config.db = nil

    e.up.WithLabelValues(config.Database,config.Instance).Set(0)
  }
}

// Close Connections
func (e *Exporter) Close() {
  if e.config.db != nil {
    e.config.db.Close()
    e.config.db = nil
  }
}


// Collect implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
  var err error

  defer func(begun time.Time) {
    e.duration.WithLabelValues(e.config.Database,e.config.Instance).Set(time.Since(begun).Seconds())
    if err == nil {
      e.error.WithLabelValues(e.config.Database,e.config.Instance).Set(0)
    } else {
      e.error.WithLabelValues(e.config.Database,e.config.Instance).Set(1)
    }
  }(time.Now())

  e.Connect()
  e.totalScrapes.WithLabelValues(e.config.Database,e.config.Instance).Inc()
  defer e.Close()

  e.up.Collect(ch)

  e.ScrapeUptime()
  e.uptime.Collect(ch)

  e.ScrapeSession()
  e.session.Collect(ch)

  e.ScrapeSysstat()
  e.sysstat.Collect(ch)

  e.ScrapeWaitclass()
  e.waitclass.Collect(ch)

  e.ScrapeSysmetric()
  e.sysmetric.Collect(ch)

  e.ScrapeTablespace()
  e.tablespace.Collect(ch)

  e.ScrapeInterconnect()
  e.interconnect.Collect(ch)

  e.ScrapeRecovery()
  e.recovery.Collect(ch)

  e.ScrapeRedo()
  e.redo.Collect(ch)

  e.ScrapeCache()
  e.cache.Collect(ch)

  e.ScrapeServices()
  e.services.Collect(ch)

  e.ScrapeParameter()
  e.parameter.Collect(ch)

  e.ScrapeQuery()
  e.query.Collect(ch)

  e.ScrapeAsmspace()
  e.asmspace.Collect(ch)

  e.duration.Collect(ch)
  e.totalScrapes.Collect(ch)
  e.error.Collect(ch)
  e.scrapeErrors.Collect(ch)
}

func (e *Exporter) Handler(w http.ResponseWriter, r *http.Request) {
  prometheus.Handler().ServeHTTP(w, r)
}

func ScrapeHandler(w http.ResponseWriter, r *http.Request) {
  target := r.URL.Query().Get("target")

 
  for _, conn := range configs.Cfgs {
     if conn.Connection == target {
        if handlers[target] == nil {
          registry := prometheus.NewRegistry()
          exporter := NewExporter()
          exporter.config = conn
          registry.MustRegister(exporter)
          handlers[target] = promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
        }

     }
  }

  // Delegate http serving to Prometheus client library, which will call collector.Collect.
  h := handlers[target]

  if h == nil {
    http.Error(w, fmt.Sprintf("Target not found %v", target), 400)
    return
  } 

  h.ServeHTTP(w, r)
}

func LoadConfig() bool {
  content, err := ioutil.ReadFile(*configFile)
  if err != nil {
      log.Fatalf("error: %v", err)
      return false
  } else {
    err := yaml.Unmarshal(content, &configs)
    if err != nil {
      log.Fatalf("error: %v", err)
      return false
    }
    return true
  }
}

func main() {
  flag.Parse()
  log.Infoln("Starting Prometheus Oracle exporter " + Version)
  metricsExporter = NewExporter()

  if LoadConfig() {
    log.Infoln("Config loaded: ", *configFile)

    http.HandleFunc(*metricPath, ScrapeHandler)
    http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {w.Write(landingPage)})

    log.Infoln("Listening on", *listenAddress)
    log.Fatal(http.ListenAndServe(*listenAddress, nil))
  }
}
