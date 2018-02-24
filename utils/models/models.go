package models

const (
	GlobalKeyName = "name"
	KeyCore       = "core"
	KeyHostName   = "hostname"
	KeyOsInfo     = "osinfo"
	KeyLocalIp    = "localip"

	ContentTypeHeader     = "Content-Type"
	ContentEncodingHeader = "Content-Encoding"

	ApplicationJson = "application/json"
	ApplicationGzip = "application/gzip"

	KeyPandoraStash = "pandora_stash" // 当只有一条数据且 sendError 时候，将其转化为 raw 发送到 pandora_stash 这个字段
)

type Option struct {
	KeyName       string
	ChooseOnly    bool
	ChooseOptions []interface{}
	Default       interface{}
	DefaultNoUse  bool
	Description   string
	CheckRegex    string
	Type          string `json:"Type,omitempty"`
	Secret        bool
	Advance       bool   `json:"advance,omitempty"`
	AdvanceDepend string `json:"advance_depend,omitempty"`
}

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Data store as use key/value map
type Data map[string]interface{}
