package main

import (
    "strings"
    "database/sql"
)

type Query struct {
  Sql string         `yaml:"sql"`
  Name string        `yaml:"name"`
}

type Config struct {
  Connection string  `yaml:"connection"`
  User string        `yaml:"user"`
  Password string    `yaml:"password"`
  Queries []Query    `yaml:"queries"`
  db                 *sql.DB
  Instance string
  Database string
}

type Configs struct {
  Cfgs []Config `yaml:"connections"`
}

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
