package storage

type Payload struct {
    Date       int64    `json:"date"`
    ParcelId   int       `json:"parcelId"`
    Bucket     string    `json:"bucket"`
    ImageLevel string    `json:"imageLevel"`
    ImageType  string    `json:"imageType"`
}