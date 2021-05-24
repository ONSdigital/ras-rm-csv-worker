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
)

type Party struct {
	SAMPLEUNITREF string `json:"sampleUnitRef"`
	SAMPLESUMMARYID string `json:"sampleSummaryId"`
	SAMPLEUNITTYPE string `json:"sampleUnitType""`
	attributes Attributes `json:attributes`

	msg             *pubsub.Message `json:"-"`
}

type Attributes struct {
	CHECKLETTER   string `json:"checkletter,omitempty"`
	FROSIC92      string `json:"frosic92,omitempty"`
	RUSIC92       string `json:"rusic92,omitempty"`
	FROSIC2007    string `json:"frosic2007,omitempty"`
	RUSIC2007     string `json:"rusic2007,omitempty"`
	FROEMPMENT    string `json:"froempment,omitempty"`
	FROTOVER      string `json:"frotover,omitempty"`
	ENTREF        string `json:"entref,omitempty"`
	LEGALSTATUS   string `json:"legalstatus,omitempty"`
	NAME		  string `json:"name,omitempty"`
	ENTREPMKR     string `json:"entrepmkr,omitempty"`
	REGION        string `json:"region,omitempty"`
	BIRTHDATE     string `json:"birthdate,omitempty"`
	ENTNAME1      string `json:"entname1,omitempty"`
	ENTNAME2      string `json:"entname2,omitempty"`
	ENTNAME3      string `json:"entname3,omitempty"`
	RUNAME1       string `json:"runame1,omitempty"`
	RUNAME2       string `json:"runame2,omitempty"`
	RUNAME3       string `json:"runame3,omitempty"`
	TRADSTYLE1    string `json:"tradstyle1,omitempty"`
	TRADSTYLE2    string `json:"tradstyle2,omitempty"`
	TRADSTYLE3    string `json:"tradstyle3,omitempty"`
	SELTYPE       string `json:"seltype,omitempty"`
	INCLEXCL      string `json:"inclexcl,omitempty"`
	CELLNO        string `json:"cellNo,omitempty"`
	FORMTYPE      string `json:"formType,omitempty"`
	CURRENCY      string `json:"currency,omitempty"`
	SAMPLEUNITID  string `json:"sampleUnitRef,omitempty"`

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
		CHECKLETTER:   sample[1],
		FROSIC92:      sample[2],
		RUSIC92:       sample[3],
		FROSIC2007:    sample[4],
		RUSIC2007:     sample[5],
		FROEMPMENT:    sample[6],
		FROTOVER:      sample[7],
		ENTREF:        sample[8],
		LEGALSTATUS:   sample[9],
		NAME:		   "",
		ENTREPMKR:     sample[10],
		REGION:        sample[11],
		BIRTHDATE:     sample[12],
		ENTNAME1:      sample[13],
		ENTNAME2:      sample[14],
		ENTNAME3:      sample[15],
		RUNAME1:       sample[16],
		RUNAME2:       sample[17],
		RUNAME3:       sample[18],
		TRADSTYLE1:    sample[19],
		TRADSTYLE2:    sample[20],
		TRADSTYLE3:    sample[21],
		SELTYPE:       sample[22],
		INCLEXCL:      sample[23],
		CELLNO:        sample[24],
		FORMTYPE:      sample[25],
		CURRENCY:      sample[26],
		SAMPLEUNITID:  sampleUnitId,
	}
	party := &Party{
		SAMPLEUNITREF: sample[0],
		SAMPLESUMMARYID: sampleSummaryId,
		SAMPLEUNITTYPE: "B",
		attributes: *attr,

	}
	logger.Debug("party created", zap.String("SAMPLEUNITREF", party.SAMPLEUNITREF))
	return party
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
