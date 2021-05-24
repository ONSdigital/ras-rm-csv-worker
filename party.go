package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"encoding/csv"
	"encoding/json"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"fmt"
	"errors"
	"strconv"
)

type Party struct {
	SAMPLEUNITREF string `json:"sampleUnitRef"`
	SAMPLESUMMARYID string `json:"sampleSummaryId"`
	SAMPLEUNITTYPE string `json:"sampleUnitType""`
	Attributes 		Attributes `json:"attributes""`
	msg             *pubsub.Message `json:"-"`
}

type Attributes struct {
	CHECKLETTER   string `json:"checkletter"`
	FROSIC92      string `json:"frosic92"`
	RUSIC92       string `json:"rusic92"`
	FROSIC2007    string `json:"frosic2007"`
	RUSIC2007     string `json:"rusic2007"`
	FROEMPMENT    int `json:"froempment"`
	FROTOVER      int `json:"frotover"`
	ENTREF        string `json:"entref"`
	LEGALSTATUS   string `json:"legalstatus"`
	NAME		  string `json:"name"`
	ENTREPMKR     string `json:"entrepmkr"`
	REGION        string `json:"region"`
	BIRTHDATE     string `json:"birthdate"`
	ENTNAME1      string `json:"entname1"`
	ENTNAME2      string `json:"entname2"`
	ENTNAME3      string `json:"entname3"`
	RUNAME1       string `json:"runame1"`
	RUNAME2       string `json:"runame2"`
	RUNAME3       string `json:"runame3"`
	TRADSTYLE1    string `json:"tradstyle1"`
	TRADSTYLE2    string `json:"tradstyle2"`
	TRADSTYLE3    string `json:"tradstyle3"`
	SELTYPE       string `json:"seltype"`
	INCLEXCL      string `json:"inclexcl"`
	CELLNO        int `json:"cellNo"`
	FORMTYPE      string `json:"formType"`
	CURRENCY      string `json:"currency"`
	SAMPLEUNITID  string `json:"sampleUnitRef"`

}

func processParty(line []byte, sampleSummaryId string, sampleUnitId string, msg *pubsub.Message) error {
	logger.Debug("processing party")
	p := newParty(line, sampleSummaryId, sampleUnitId)
	p.msg = msg
	return p.sendToPartyService()
}

func newParty(line []byte, sampleSummaryId string, sampleUnitId string) *Party {
	logger.Debug("reading csv line")
	r := csv.NewReader(bytes.NewReader(line))
	r.Comma = ':'

	sample, err := r.Read()
	if err != nil {
		logger.Fatal("unable to parse sample csv", zap.Error(err))
	}
	logger.Debug("read sample", zap.Strings("sample", sample))

	attr := &Attributes{
		CHECKLETTER:   setIfNotEmpty(&sample[1]),
		FROSIC92:      setIfNotEmpty(&sample[2]),
		RUSIC92:       setIfNotEmpty(&sample[3]),
		FROSIC2007:    setIfNotEmpty(&sample[4]),
		RUSIC2007:     setIfNotEmpty(&sample[5]),
		FROEMPMENT:    convertToInt(sample[6]),
		FROTOVER:      convertToInt(sample[7]),
		ENTREF:        setIfNotEmpty(&sample[8]),
		LEGALSTATUS:   setIfNotEmpty(&sample[9]),
		NAME:		   "",
		ENTREPMKR:     setIfNotEmpty(&sample[10]),
		REGION:        setIfNotEmpty(&sample[11]),
		BIRTHDATE:     setIfNotEmpty(&sample[12]),
		ENTNAME1:      setIfNotEmpty(&sample[13]),
		ENTNAME2:      setIfNotEmpty(&sample[14]),
		ENTNAME3:      setIfNotEmpty(&sample[15]),
		RUNAME1:       setIfNotEmpty(&sample[16]),
		RUNAME2:       setIfNotEmpty(&sample[17]),
		RUNAME3:       setIfNotEmpty(&sample[18]),
		TRADSTYLE1:    setIfNotEmpty(&sample[19]),
		TRADSTYLE2:    setIfNotEmpty(&sample[20]),
		TRADSTYLE3:    setIfNotEmpty(&sample[21]),
		SELTYPE:       setIfNotEmpty(&sample[22]),
		INCLEXCL:      setIfNotEmpty(&sample[23]),
		CELLNO:        convertToInt(sample[24]),
		FORMTYPE:      setIfNotEmpty(&sample[25]),
		CURRENCY:      setIfNotEmpty(&sample[26]),
		SAMPLEUNITID:  sampleUnitId,
	}
	party := &Party{
		SAMPLEUNITREF: sample[0],
		SAMPLESUMMARYID: sampleSummaryId,
		SAMPLEUNITTYPE: "B",
		Attributes: *attr,
	}
	logger.Debug("party created", zap.String("SAMPLEUNITREF", party.SAMPLEUNITREF))
	return party
}

func setIfNotEmpty(value *string) string {
	if value == nil {
		return ""
	} else {
		return *value
	}
}

func convertToInt(value string) int {
	v, err := strconv.Atoi(value)
	if err != nil {
		logger.Error("error converting to int",  zap.Error(err))
		v = 0
	}
	return v
}

func (p *Party) sendToPartyService() error {
	payload, err := p.marshall()
	if err != nil {
		return err
	}
	sampleServiceUrl := p.getPartyServiceUrl()
	return p.sendHttpRequest(sampleServiceUrl, payload)
}

func (p Party) marshall() ([]byte, error) {
	//marshall to JSON and send to the sample service as a POST request
	payload, err := json.Marshal(p)
	logger.Debug("marshalled sample to json", zap.ByteString("payload", payload))
	if err != nil {
		logger.Error("unable to marshall sample to json", zap.Error(err))
		return nil, err
	}
	return payload, nil
}

func (p Party) getPartyServiceUrl() string {
	partyServiceBaseUrl := viper.GetString("PARTY_SERVICE_BASE_URL")
	partyServicePath := "/party-api/v1/parties"
	partyServiceUrl := partyServiceBaseUrl + partyServicePath
	logger.Info("using sample service url", zap.String("url", partyServiceUrl))
	return partyServiceUrl
}

func (p Party) sendHttpRequest(url string, payload []byte) error {
	//resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	username := viper.GetString("SECURITY_USER_NAME")
	password := viper.GetString("SECURITY_USER_PASSWORD")
	client := &http.Client{}
	req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
	if err != nil {
		logger.Error("error creating HTTP request", zap.Error(err))
		return err
	}
	req.SetBasicAuth(username, password)
	req.Header.Add("content-type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("error sending HTTP request", zap.Error(err))
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Error("error reading HTTP response", zap.Error(err))
		return err
	}
	logger.Debug("response received", zap.ByteString("body", body))
	if resp.StatusCode == http.StatusCreated {
		logger.Info("party created", zap.String("sampleUnitRef", p.SAMPLEUNITREF), zap.String("messageId", p.msg.ID))
		return nil
	} else if resp.StatusCode == http.StatusConflict {
		logger.Warn("attempted to create duplicate sample", zap.Int("status code", resp.StatusCode), zap.String("sampleUnitRef", p.SAMPLEUNITREF), zap.String("messageId", p.msg.ID))
		// if this sample unit has already been created ack the message to stop it being recreated
		return nil
	} else {
		logger.Error("party not created status", zap.Int("status code", resp.StatusCode), zap.String("sampleUnitRef", p.SAMPLEUNITREF), zap.String("messageId", p.msg.ID))
		return errors.New(fmt.Sprintf("sample not created - status code %d", resp.StatusCode))
	}
}
