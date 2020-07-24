package main

import (
	"fmt"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestMain(m *testing.M) {

	// call flag.Parse() here if TestMain uses flags
	os.Exit(m.Run())
}

func TestSampleSuccess(t *testing.T) {
	assert := assert.New(t)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("OK"))
	}))
	ts.Start()
	defer ts.Close()

	fmt.Printf("Setting sample service base url %v", ts.URL)
	viper.Set("SAMPLE_SERVICE_BASE_URL", ts.URL)

	sample := []byte("13110000001:::::::::::WW:::::OFFICE FOR NATIONAL STATISTICS:::::::::0001:")
	err := processSample(sample)
	assert.Nil(err, "error should be nil")
}

func TestSampleError(t *testing.T) {
	assert := assert.New(t)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("FAILED"))
	}))
	ts.Start()
	defer ts.Close()

	fmt.Printf("Setting sample service base url %v", ts.URL)
	viper.Set("SAMPLE_SERVICE_BASE_URL", ts.URL)

	sample := []byte("13110000001:::::::::::WW:::::OFFICE FOR NATIONAL STATISTICS:::::::::0001:")
	err := processSample(sample)
	assert.NotNil(t, err, "error should not be nil")
}

func TestSampleServerURL(t *testing.T) {
	s := &Sample{}
	assert := assert.New(t)
	// set env variables and check url is correct
	viper.Set("SAMPLE_SERVICE_BASE_URL", "https://127.0.0.1")
	viper.Set("SAMPLE_SERVICE_PATH", "/test")
	assert.Equal("https://127.0.0.1/test", s.getSampleServiceUrl())
}

func TestSendHttpRequest(t *testing.T) {
	s := &Sample{}
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
	err := s.sendHttpRequest(ts.URL, payload)
	assert.Nil(err, "error should be nil")
}

func TestSendHttpRequestBadUrl(t *testing.T) {
	s := &Sample{}
	assert := assert.New(t)
	payload := []byte("TEST")
	err := s.sendHttpRequest("http://localhost", payload)
	assert.NotNil(err, "error should be nil")
}

func TestSendHttpRequestWrongStatus(t *testing.T) {
	s := &Sample{}
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
	err := s.sendHttpRequest(ts.URL, payload)
	assert.NotNil(err, "error should be nil")
}

func TestSendSampleSuccess(t *testing.T) {
	assert := assert.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("OK"))
	}))
	defer ts.Close()

	fmt.Printf("Setting sample service base url %v\n", ts.URL)
	viper.Set("SAMPLE_SERVICE_BASE_URL", ts.URL)

	s := createSample()
	err := s.sendToSampleService()
	assert.Nil(err, "error should be nil")
}

func TestMarshall(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	s := createSample()

	sample, err := s.marshall()
	assert.Nil(err)
	sampleJson := "{\"SAMPLEUNITREF\":\"111\"," +
		"\"CHECKLETTER\":\"W\"," +
		"\"FROSIC92\":\"92\"," +
		"\"RUSIC92\":\"r92\"," +
		"\"FROSIC2007\":\"2007\"," +
		"\"RUSIC2007\":\"r2007\"," +
		"\"FROEMPMENT\":\"010120\"," +
		"\"FROTOVER\":\"over\"," +
		"\"ENTREF\":\"ref\"," +
		"\"LEGALSTATUS\":\"stats\"," +
		"\"ENTREPMKR\":\"mkr\"," +
		"\"REGION\":\"gb\"," +
		"\"BIRTHDATE\":\"010180\"," +
		"\"ENTNAME1\":\"name1\"," +
		"\"ENTNAME2\":\"name2\"," +
		"\"ENTNAME3\":\"name3\"," +
		"\"RUNAME1\":\"ru1\"," +
		"\"RUNAME2\":\"ru2\"," +
		"\"RUNAME3\":\"ru3\"," +
		"\"TRADSTYLE1\":\"trad1\"," +
		"\"TRADSTYLE2\":\"trad2\"," +
		"\"TRADSTYLE3\":\"trad3\"," +
		"\"SELTYPE\":\"type\"," +
		"\"INCLEXCL\":\"inc\"," +
		"\"CELLNO\":\"123\"," +
		"\"FORMTYPE\":\"0001\"," +
		"\"CURRENCY\":\"£\"}"

	assert.Equal(sampleJson, string(sample))
}

func TestMarshallEmptyStruct(t *testing.T) {
	t.Parallel()
	s := &Sample{}
	assert := assert.New(t)

	sample, err := s.marshall()
	assert.Nil(err)
	emptySample := createEmptySample()
	assert.Equal(emptySample, string(sample))
}

func createSample() *Sample {
	s := &Sample{}
	s.BIRTHDATE = "010180"
	s.CELLNO = "123"
	s.CHECKLETTER = "W"
	s.CURRENCY = "£"
	s.ENTNAME1 = "name1"
	s.ENTNAME2 = "name2"
	s.ENTNAME3 = "name3"
	s.ENTREF = "ref"
	s.ENTREPMKR = "mkr"
	s.FORMTYPE = "0001"
	s.FROEMPMENT = "010120"
	s.FROSIC92 = "92"
	s.FROSIC2007 = "2007"
	s.FROTOVER = "over"
	s.INCLEXCL = "inc"
	s.LEGALSTATUS = "stats"
	s.REGION = "gb"
	s.RUNAME1 = "ru1"
	s.RUNAME2 = "ru2"
	s.RUNAME3 = "ru3"
	s.RUSIC92 = "r92"
	s.RUSIC2007 = "r2007"
	s.SAMPLEUNITREF = "111"
	s.SELTYPE = "type"
	s.TRADSTYLE1 = "trad1"
	s.TRADSTYLE2 = "trad2"
	s.TRADSTYLE3 = "trad3"
	return s
}

func createEmptySample() string {
	return "{\"SAMPLEUNITREF\":\"\"," +
		"\"CHECKLETTER\":\"\"," +
		"\"FROSIC92\":\"\"," +
		"\"RUSIC92\":\"\"," +
		"\"FROSIC2007\":\"\"," +
		"\"RUSIC2007\":\"\"," +
		"\"FROEMPMENT\":\"\"," +
		"\"FROTOVER\":\"\"," +
		"\"ENTREF\":\"\"," +
		"\"LEGALSTATUS\":\"\"," +
		"\"ENTREPMKR\":\"\"," +
		"\"REGION\":\"\"," +
		"\"BIRTHDATE\":\"\"," +
		"\"ENTNAME1\":\"\"," +
		"\"ENTNAME2\":\"\"," +
		"\"ENTNAME3\":\"\"," +
		"\"RUNAME1\":\"\"," +
		"\"RUNAME2\":\"\"," +
		"\"RUNAME3\":\"\"," +
		"\"TRADSTYLE1\":\"\"," +
		"\"TRADSTYLE2\":\"\"," +
		"\"TRADSTYLE3\":\"\"," +
		"\"SELTYPE\":\"\"," +
		"\"INCLEXCL\":\"\"," +
		"\"CELLNO\":\"\"," +
		"\"FORMTYPE\":\"\"," +
		"\"CURRENCY\":\"\"}"
}
