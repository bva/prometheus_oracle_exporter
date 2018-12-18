package main

import (
    "os"
    "time"
    "strings"
    "io/ioutil"
    "gopkg.in/yaml.v2"
    "database/sql"
    "github.com/prometheus/common/log"
  _ "github.com/mattn/go-oci8"
)

type Alert struct {
  File string        `yaml:"file"`
  Ignoreora []string `yaml:"ignoreora"`
}

type Query struct {
  Sql string         `yaml:"sql"`
  Name string        `yaml:"name"`
}

type Config struct {
  Connection string  `yaml:"connection"`
  Database string    `yaml:"database"`
  Instance string    `yaml:"instance"`
  Alertlog []Alert   `yaml:"alertlog"`
  Queries []Query    `yaml:"queries"`
  db                 *sql.DB
}

type Configs struct {
  Cfgs []Config `yaml:"connections"`
}

var (
   pwd           string
)

// Oracle gives us some ugly names back. This function cleans things up for Prometheus.
func cleanName(s string) string {
  s = strings.Replace(s, " ", "_", -1) // Remove spaces
  s = strings.Replace(s, "(", "", -1)  // Remove open parenthesis
  s = strings.Replace(s, ")", "", -1)  // Remove close parenthesis
  s = strings.Replace(s, "/", "", -1)  // Remove forward slashes
  s = strings.ToLower(s)
  return s
}

func cleanIp(s string) string {
  s = strings.Replace(s, ":", "", -1) // Remove spaces
  s = strings.Replace(s, ".", "_", -1)  // Remove open parenthesis
  return s
}

func ReadAccess(){
  var file = pwd + "/" + *accessFile
  content, err := ioutil.ReadFile(file)
  if err == nil {
    err := yaml.Unmarshal(content, &lastlog)
    if err != nil {
      log.Fatalf("error1: %v", err)
    }
  }
}

func WriteAccess(){
  content, _ := yaml.Marshal(&lastlog)
  ioutil.WriteFile(pwd + "/" + *accessFile, content, 0644)
}

func WriteLog(message string) {
  fh, err := os.OpenFile(pwd + "/" + *logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
  if err == nil {
    fh.Seek(0,2)
    fh.WriteString(time.Now().Format("2006-01-02 15:04:05") + " " + message + "\n")
    fh.Close()
  }
}
