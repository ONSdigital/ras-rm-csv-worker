package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"encoding/json"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"

	"github.com/spf13/viper"
)

type Sample struct {
	SAMPLEUNITREF string `json:"sampleUnitRef"`
	CHECKLETTER   string `json:"checkletter"`
	FROSIC92      string `json:"frosic92"`
	RUSIC92       string `json:"rusic92"`
	FROSIC2007    string `json:"frosic2007"`
	RUSIC2007     string `json:"rusic2007"`
	FROEMPMENT    string `json:"froempment"`
	FROTOVER      string `json:"frotover"`
	ENTREF        string `json:"entref"`
	LEGALSTATUS   string `json:"legalstatus"`
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
	CELLNO        string `json:"cellNo"`
	FORMTYPE      string `json:"formType"`
	CURRENCY      string `json:"currency"`

	sampleSummaryId string          `json:"-"`
	msg             *pubsub.Message `json:"-"`
}

func processSample(line []string, sampleSummaryId string, msg *pubsub.Message) (string, error) {
	logger.Debug("processing sample")
	s := create(line)
	s.sampleSummaryId = sampleSummaryId
	s.msg = msg
	return s.sendToSampleService()
}

func create(line []string) *Sample {
	sampleUnit := &Sample{
		SAMPLEUNITREF: line[0],
		CHECKLETTER:   line[1],
		FROSIC92:      line[2],
		RUSIC92:       line[3],
		FROSIC2007:    line[4],
		RUSIC2007:     line[5],
		FROEMPMENT:    line[6],
		FROTOVER:      line[7],
		ENTREF:        line[8],
		LEGALSTATUS:   line[9],
		ENTREPMKR:     line[10],
		REGION:        line[11],
		BIRTHDATE:     line[12],
		ENTNAME1:      line[13],
		ENTNAME2:      line[14],
		ENTNAME3:      line[15],
		RUNAME1:       line[16],
		RUNAME2:       line[17],
		RUNAME3:       line[18],
		TRADSTYLE1:    line[19],
		TRADSTYLE2:    line[20],
		TRADSTYLE3:    line[21],
		SELTYPE:       line[22],
		INCLEXCL:      line[23],
		CELLNO:        line[24],
		FORMTYPE:      line[25],
		CURRENCY:      line[26],
	}
	logger.Debug("sample created", zap.String("SAMPLEUNITREF", sampleUnit.SAMPLEUNITREF))
	return sampleUnit
}

func (s *Sample) sendToSampleService() (string, error) {
	payload, err := s.marshall()
	if err != nil {
		return "", err
	}
	sampleServiceUrl := s.getSampleServiceUrl()
	return s.sendHttpRequest(sampleServiceUrl, payload)
}

func (s Sample) marshall() ([]byte, error) {
	//marshall to JSON and send to the sample service as a POST request
	payload, err := json.Marshal(s)
	logger.Debug("marshalled sample to json", zap.ByteString("payload", payload))
	if err != nil {
		logger.Error("unable to marshall sample to json", zap.Error(err))
		return nil, err
	}
	return payload, nil
}

func (s Sample) getSampleServiceUrl() string {
	sampleServiceBaseUrl := viper.GetString("SAMPLE_SERVICE_BASE_URL")
	sampleServicePath := fmt.Sprintf("/samples/%s/sampleunits/", s.sampleSummaryId)
	sampleServiceUrl := sampleServiceBaseUrl + sampleServicePath
	logger.Info("using sample service url", zap.String("url", sampleServiceUrl))
	return sampleServiceUrl
}

func (s Sample) sendHttpRequest(url string, payload []byte) (string, error) {
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		logger.Error("error sending HTTP request", zap.Error(err))
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Error("error reading HTTP response", zap.Error(err))
		return "", err
	}
	logger.Debug("response received", zap.ByteString("body", body))
	if resp.StatusCode == http.StatusCreated {
		logger.Info("sample created", zap.String("sampleUnitRef", s.SAMPLEUNITREF), zap.String("messageId", s.msg.ID))
		data := make(map[string]interface{})
		err := json.Unmarshal(body, &data)
		if err != nil {
			logger.Error("error decoding JSON response", zap.Error(err))
		}

		sampleUnitId, ok := data["id"].(string)
		if !ok {
			logger.Error("missing sample unit id", zap.String("sampleUnitRef", s.SAMPLEUNITREF), zap.String("messageId", s.msg.ID))
			sampleUnitId = ""
		}
		return sampleUnitId, nil

	} else if resp.StatusCode == http.StatusConflict {
		logger.Warn("attempted to create duplicate sample", zap.Int("status code", resp.StatusCode), zap.String("sampleUnitRef", s.SAMPLEUNITREF), zap.String("messageId", s.msg.ID))
		// if this sample unit has already been created ack the message to stop it being recreated
		return "", nil
	} else {
		logger.Error("sample not created status", zap.Int("status code", resp.StatusCode), zap.String("sampleUnitRef", s.SAMPLEUNITREF), zap.String("messageId", s.msg.ID))
		return "", errors.New(fmt.Sprintf("sample not created - status code %d", resp.StatusCode))
	}
}
