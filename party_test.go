package main

import (
	"fmt"
	"io/ioutil"
	"testing"
	"cloud.google.com/go/pubsub"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
)

func TestPartySuccess(t *testing.T) {
	assert := assert.New(t)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("OK"))
	}))
	ts.Start()
	defer ts.Close()

	fmt.Printf("Setting party service base url %v", ts.URL)
	viper.Set("PARTY_SERVICE_BASE_URL", ts.URL)

	line := []byte("13110000001:::::::::::WW:::::OFFICE FOR NATIONAL STATISTICS:::::::::0001:")

	msg := &pubsub.Message{
		Data: []byte(line),
		Attributes: map[string]string{
			"sample_summary_id": "test",
		},
		ID: "1",
	}
	sample := readSampleLine(line)
	err := processParty(sample, "test", "test", msg)
	assert.Nil(err, "error should be nil")
}


func TestPartyError(t *testing.T) {
	assert := assert.New(t)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("ERROR"))
	}))
	ts.Start()
	defer ts.Close()

	fmt.Printf("Setting party service base url %v", ts.URL)
	viper.Set("PARTY_SERVICE_BASE_URL", ts.URL)

	line := []byte("13110000001:::::::::::WW:::::OFFICE FOR NATIONAL STATISTICS:::::::::0001:")

	msg := &pubsub.Message{
		Data: []byte(line),
		Attributes: map[string]string{
			"sample_summary_id": "test",
		},
		ID: "1",
	}
	sample := readSampleLine(line)
	err := processParty(sample, "test", "test", msg)
	assert.NotNil(err, "error should be nil")
}

func TestPartyServerURL(t *testing.T) {
	p := &Party{}

	assert := assert.New(t)
	// set env variables and check url is correct
	viper.Set("PARTY_SERVICE_BASE_URL", "https://127.0.0.1")

	assert.Equal("https://127.0.0.1/party-api/v1/parties", p.getPartyServiceUrl())
}


func TestPartySendHttpRequest(t *testing.T) {
	p := &Party{}
	p.msg = &pubsub.Message{
		Data: []byte(line),
		Attributes: map[string]string{
			"sample_summary_id": "test",
		},
		ID: "1",
	}
	assert := assert.New(t)
	payload := []byte("TEST")

	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		assert.Nil(err)
		assert.Equal(payload, body)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("OK"))
	}))
	ts.Start()
	defer ts.Close()
	err := p.sendHttpRequest(ts.URL, payload)
	assert.Nil(err, "error should be nil")
}

func TestPartySendHttpRequestBadUrl(t *testing.T) {
	p := &Party{}
	p.msg = &pubsub.Message{
		Data: []byte(line),
		Attributes: map[string]string{
			"sample_summary_id": "test",
		},
		ID: "1",
	}

	assert := assert.New(t)
	payload := []byte("TEST")
	err := p.sendHttpRequest("http://localhost", payload)
	assert.NotNil(err, "error should be nil")
}

func TestPartySendHttpRequestWrongStatus(t *testing.T) {
	p := &Party{}
	p.msg = &pubsub.Message{
		Data: []byte(line),
		Attributes: map[string]string{
			"sample_summary_id": "test",
		},
		ID: "1",
	}

	assert := assert.New(t)
	payload := []byte("TEST")

	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		assert.Nil(err)
		assert.Equal(payload, body)
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("OK"))
	}))
	ts.Start()
	defer ts.Close()
	err := p.sendHttpRequest(ts.URL, payload)
	assert.NotNil(err, "error should be nil")
}