package main

import (
	"cloud.google.com/go/pubsub"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestSampleSuccess(t *testing.T) {
	assert := assert.New(t)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("{\"id\":\"1111\"}"))
	}))
	ts.Start()
	defer ts.Close()

	fmt.Printf("Setting sample service base url %v", ts.URL)
	viper.Set("SAMPLE_SERVICE_BASE_URL", ts.URL)

	line := []byte("13110000001:::::::::::WW:::::OFFICE FOR NATIONAL STATISTICS:::::::::0001:")

	msg := &pubsub.Message{
		Data: []byte(line),
		Attributes: map[string]string{
			"sample_summary_id": "test",
		},
		ID: "1",
	}
	sample, _ := readSampleLine(line)
	_, err := processSample(sample, "test", msg)
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

	line := []byte("13110000001:::::::::::WW:::::OFFICE FOR NATIONAL STATISTICS:::::::::0001:")

	msg := &pubsub.Message{
		Data: []byte(line),
		Attributes: map[string]string{
			"sample_summary_id": "test",
		},
		ID: "1",
	}

	sample, _ := readSampleLine(line)
	_, err := processSample(sample, "test", msg)
	assert.NotNil(t, err, "error should not be nil")
}

func TestSampleServerURL(t *testing.T) {
	s := &Sample{}
	s.msg = &pubsub.Message{
		Data: []byte(line),
		Attributes: map[string]string{
			"sample_summary_id": "test",
		},
		ID: "1",
	}

	assert := assert.New(t)
	// set env variables and check url is correct
	viper.Set("SAMPLE_SERVICE_BASE_URL", "https://127.0.0.1")
	s.sampleSummaryId = "test"
	assert.Equal("https://127.0.0.1/samples/test/sampleunits/", s.getSampleServiceUrl())
}

func TestSendHttpRequest(t *testing.T) {
	s := &Sample{}
	s.msg = &pubsub.Message{
		Data: []byte(line),
		Attributes: map[string]string{
			"sample_summary_id": "test",
		},
		ID: "1",
	}
	assert := assert.New(t)
	payload := []byte("TEST")

	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		assert.Nil(err)
		assert.Equal(payload, body)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("{\"id\":\"1111\"}"))
	}))
	ts.Start()
	defer ts.Close()
	_, err := s.sendHttpRequest(ts.URL, payload)
	assert.Nil(err, "error should be nil")
}

func TestSendHttpRequestBadUrl(t *testing.T) {
	s := &Sample{}
	s.msg = &pubsub.Message{
		Data: []byte(line),
		Attributes: map[string]string{
			"sample_summary_id": "test",
		},
		ID: "1",
	}
	assert := assert.New(t)
	payload := []byte("TEST")
	_, err := s.sendHttpRequest("http://localhost", payload)
	assert.NotNil(err, "error should be nil")
}

func TestSendHttpRequestWrongStatus(t *testing.T) {
	s := &Sample{}
	s.msg = &pubsub.Message{
		Data: []byte(line),
		Attributes: map[string]string{
			"sample_summary_id": "test",
		},
		ID: "1",
	}
	assert := assert.New(t)
	payload := []byte("TEST")

	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		assert.Nil(err)
		assert.Equal(payload, body)
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("{\"id\":\"1111\"}"))
	}))
	ts.Start()
	defer ts.Close()
	_, err := s.sendHttpRequest(ts.URL, payload)
	assert.NotNil(err, "error should be nil")
}

func TestSendSampleSuccess(t *testing.T) {
	assert := assert.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("{\"id\":\"1111\"}"))
	}))
	defer ts.Close()

	fmt.Printf("Setting sample service base url %v\n", ts.URL)
	viper.Set("SAMPLE_SERVICE_BASE_URL", ts.URL)

	s := createSample()
	_, err := s.sendToSampleService()
	assert.Nil(err, "error should be nil")
}

func TestMarshall(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	s := createSample()

	sample, err := s.marshall()
	assert.Nil(err)
	sampleJson := "{\"sampleUnitRef\":\"111\"," +
		"\"checkletter\":\"W\"," +
		"\"frosic92\":\"92\"," +
		"\"rusic92\":\"r92\"," +
		"\"frosic2007\":\"2007\"," +
		"\"rusic2007\":\"r2007\"," +
		"\"froempment\":\"010120\"," +
		"\"frotover\":\"over\"," +
		"\"entref\":\"ref\"," +
		"\"legalstatus\":\"stats\"," +
		"\"entrepmkr\":\"mkr\"," +
		"\"region\":\"gb\"," +
		"\"birthdate\":\"010180\"," +
		"\"entname1\":\"name1\"," +
		"\"entname2\":\"name2\"," +
		"\"entname3\":\"name3\"," +
		"\"runame1\":\"ru1\"," +
		"\"runame2\":\"ru2\"," +
		"\"runame3\":\"ru3\"," +
		"\"tradstyle1\":\"trad1\"," +
		"\"tradstyle2\":\"trad2\"," +
		"\"tradstyle3\":\"trad3\"," +
		"\"seltype\":\"type\"," +
		"\"inclexcl\":\"inc\"," +
		"\"cellNo\":\"123\"," +
		"\"formType\":\"0001\"," +
		"\"currency\":\"£\"}"

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

func TestGetSampleUnit(t *testing.T) {
	configureLogging()
	assert := assert.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{\"id\":\"1111\"}"))
	}))
	defer ts.Close()

	fmt.Printf("Setting sample service base url %v\n", ts.URL)
	viper.Set("SAMPLE_SERVICE_BASE_URL", ts.URL)

	s := createSample()
	id, err := s.getSampleUnitID()
	assert.Nil(err, "error should be nil")
	assert.Equal("1111", id)
}

func TestGetSampleUnitErrorResponse(t *testing.T) {
	configureLogging()
	assert := assert.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	fmt.Printf("Setting sample service base url %v\n", ts.URL)
	viper.Set("SAMPLE_SERVICE_BASE_URL", ts.URL)

	s := createSample()
	_, err := s.getSampleUnitID()
	assert.NotNil(err, "error should be not nil")
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
	s.msg = &pubsub.Message{
		Data: []byte(line),
		Attributes: map[string]string{
			"sample_summary_id": "test",
		},
		ID: "1",
	}
	return s
}

func createEmptySample() string {
	return "{\"sampleUnitRef\":\"\"," +
		"\"checkletter\":\"\"," +
		"\"frosic92\":\"\"," +
		"\"rusic92\":\"\"," +
		"\"frosic2007\":\"\"," +
		"\"rusic2007\":\"\"," +
		"\"froempment\":\"\"," +
		"\"frotover\":\"\"," +
		"\"entref\":\"\"," +
		"\"legalstatus\":\"\"," +
		"\"entrepmkr\":\"\"," +
		"\"region\":\"\"," +
		"\"birthdate\":\"\"," +
		"\"entname1\":\"\"," +
		"\"entname2\":\"\"," +
		"\"entname3\":\"\"," +
		"\"runame1\":\"\"," +
		"\"runame2\":\"\"," +
		"\"runame3\":\"\"," +
		"\"tradstyle1\":\"\"," +
		"\"tradstyle2\":\"\"," +
		"\"tradstyle3\":\"\"," +
		"\"seltype\":\"\"," +
		"\"inclexcl\":\"\"," +
		"\"cellNo\":\"\"," +
		"\"formType\":\"\"," +
		"\"currency\":\"\"}"
}
