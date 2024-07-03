package models

type SystemConfig struct {
	ThreadsNum         int    `toml:"threadsNum"`
	BufferLen          int    `toml:"bufferLen"`
	LogLevel           string `toml:"logLevel"`
	RetryCount         int    `toml:"retryCount"`
	ServerIP           string `toml:"serverIP"`
	ServerPort         string `toml:"serverPort"`
	ServerRootPassword string `toml:"serverRootPassword"`
}
