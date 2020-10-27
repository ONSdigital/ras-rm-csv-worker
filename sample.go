package main

import (
	"bytes"
	"encoding/csv"
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

	sampleSummaryId string `json:"-"`
}

func processSample(line []byte, sampleSummaryId string) error {
	logger.Debug("processing sample")
	s := parse(line)
	s.sampleSummaryId = sampleSummaryId
	return s.sendToSampleService()
}

func parse(line []byte) *Sample {
	logger.Debug("reading csv line")
	r := csv.NewReader(bytes.NewReader(line))
	r.Comma = ':'

	sample, err := r.Read()
	if err != nil {
		logger.Fatal("unable to parse sample csv", zap.Error(err))
	}
	logger.Debug("read sample", zap.Strings("sample", sample))
	sampleUnit := &Sample{
		SAMPLEUNITREF: sample[0],
		CHECKLETTER:   sample[1],
		FROSIC92:      sample[2],
		RUSIC92:       sample[3],
		FROSIC2007:    sample[4],
		RUSIC2007:     sample[5],
		FROEMPMENT:    sample[6],
		FROTOVER:      sample[7],
		ENTREF:        sample[8],
		LEGALSTATUS:   sample[9],
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
	}
	logger.Debug("sample created", zap.String("SAMPLEUNITREF", sampleUnit.SAMPLEUNITREF))
	return sampleUnit
}

func (s *Sample) sendToSampleService() error {
	payload, err := s.marshall()
	if err != nil {
		return err
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

func (s Sample) sendHttpRequest(url string, payload []byte) error {
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
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
		logger.Info("sample created")
		return nil
	} else {
		logger.Error("sample not created status", zap.Int("status code", resp.StatusCode))
		return errors.New(fmt.Sprintf("sample not created - status code %d", resp.StatusCode))
	}
}
