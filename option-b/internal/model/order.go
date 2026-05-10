package model

type OrderSubmitted struct {
	PlayerID  string `json:"playerId"`
	UnitID    string `json:"unitId"`
	OrderType string `json:"orderType"`
	Payload   string `json:"payload"`
	Turn      int    `json:"turn"`
	Timestamp int64  `json:"timestamp"`
}
