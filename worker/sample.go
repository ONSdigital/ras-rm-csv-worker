package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io/ioutil"
	"net/http"
)

type Sample struct {
	SAMPLEUNITREF string
	CHECKLETTER   string
	FROSIC92      string
	RUSIC92       string
	FROSIC2007    string
	RUSIC2007     string
	FROEMPMENT    string
	FROTOVER      string
	ENTREF        string
	LEGALSTATUS   string
	ENTREPMKR     string
	REGION        string
	BIRTHDATE     string
	ENTNAME1      string
	ENTNAME2      string
	ENTNAME3      string
	RUNAME1       string
	RUNAME2       string
	RUNAME3       string
	TRADSTYLE1    string
	TRADSTYLE2    string
	TRADSTYLE3    string
	SELTYPE       string
	INCLEXCL      string
	CELLNO        string
	FORMTYPE      string
	CURRENCY      string
}

func processSample(line []byte) error {
	log.Debug("processing sample")
	s := parse(line)
	return s.sendToSampleService()
}

func parse(line []byte) *Sample {
	log.Debug("reading csv line")
	r := csv.NewReader(bytes.NewReader(line))
	r.Comma = ':'

	sample, err := r.Read()
	if err != nil {
		log.WithError(err).Fatal("unable to parse sample csv")
	}
	log.WithField("sample", sample).Debug("read sample")
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
	log.WithField("SAMPLEUNITREF", sampleUnit.SAMPLEUNITREF).Debug("sample created")
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
	log.WithField("payload", string(payload)).Debug("marshalled sample to json")
	if err != nil {
		log.WithError(err).Error("unable to marshall sample to json")
		return nil, err
	}
	return payload, nil
}

func (s Sample) getSampleServiceUrl() string {
	sampleServiceBaseUrl := viper.GetString("SAMPLE_SERVICE_BASE_URL")
	sampleServicePath := viper.GetString("SAMPLE_SERVICE_PATH")
	sampleServiceUrl := sampleServiceBaseUrl + sampleServicePath
	log.WithField("url", sampleServiceUrl).Info("using sample service url")
	return sampleServiceUrl
}

func (s Sample) sendHttpRequest(url string, payload []byte) error {
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		log.WithError(err).Error("error sending HTTP request")
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.WithError(err).Error("error reading HTTP response")
		return err
	}
	log.WithField("body", body).Debug("response received")
	if resp.StatusCode == http.StatusCreated {
		log.Info("sample created")
		return nil
	} else {
		log.WithField("status code", resp.StatusCode).Error("sample not created status")
		return errors.New(fmt.Sprintf("sample not created - status code %d", resp.StatusCode))
	}
}
