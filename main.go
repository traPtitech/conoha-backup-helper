package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"google.golang.org/api/option"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
)

type AuthInfo struct {
	Auth struct {
		PasswordCredentials struct {
			UserName string `json:"username"`
			Password string `json:"password"`
		} `json:"passwordCredentials"`
		TenantID string `json:"tenantId"`
	} `json:"auth"`
}

type AccessToken struct {
	Access struct {
		Token struct {
			ID string `json:"id"`
		} `json:"token"`
	} `json:"access"`
}

func main() {
	token, err := getConohaAPIToken()
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")))
	if err != nil {
		log.Fatal(err)
	}
	bkt := client.Bucket(os.Getenv("BUCKET_NAME"))

	containers, err := retrieveContainerList(&token)
	if err != nil {
		log.Fatal(err)
	}

	// for debug
	fmt.Println(containers)
	fmt.Println(len(containers))

	// test value
	container := "hackmd-dev"
	objects, err := retrieveObjectList(&token, &container)
	if err != nil {
		log.Fatal(err)
	}

	// for debug
	fmt.Println(objects)
	fmt.Println(len(objects))

	// test value
	objectName := "upload_134bf91dac07807a114452d69f6c8a7d.png"
	object, err := getObject(&token, &container, &objectName)
	if err != nil {
		log.Fatal(err)
	}

	err = upload2GCP(ctx, bkt, &objectName, object)
	if err != nil {
		log.Fatal(err)
	}
}

func getConohaAPIToken() (string, error) {
	url := "https://identity.tyo1.conoha.io/v2.0/tokens"
	authInfo := new(AuthInfo)
	authInfo.Auth.PasswordCredentials.UserName = os.Getenv("Conoha_USERNAME")
	authInfo.Auth.PasswordCredentials.Password = os.Getenv("Conoha_PASSWORD")
	authInfo.Auth.TenantID = os.Getenv("Conoha_TENANT_ID")

	client := &http.Client{}

	reqJSON, _ := json.Marshal(&authInfo)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(reqJSON))
	if err != nil {
		return "", err
	}

	resp, err := client.Do(req)

	respJSON := new(AccessToken)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	err = json.Unmarshal(body, &respJSON)
	if err != nil {
		return "", err
	}

	return respJSON.Access.Token.ID, nil
}

func retrieveContainerList(token *string) ([]string, error) {
	url := "https://object-storage.tyo1.conoha.io/v1/nc_" + os.Getenv("Conoha_TENANT_ID")

	client := &http.Client{}

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("X-Auth-Token", *token)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	containers := strings.Split(string(body), "\n")
	containers = containers[:len(containers)-1]

	return containers, nil
}

func retrieveObjectList(token *string, container *string) ([]string, error) {
	url := "https://object-storage.tyo1.conoha.io/v1/nc_" + os.Getenv("Conoha_TENANT_ID") + "/" + *container

	client := &http.Client{}

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("X-Auth-Token", *token)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	objects := strings.Split(string(body), "\n")
	objects = objects[:len(objects)-1]

	return objects, nil
}

func getObject(token *string, container *string, objectName *string) (*io.Reader, error) {
	url := "https://object-storage.tyo1.conoha.io/v1/nc_" + os.Getenv("Conoha_TENANT_ID") + "/" + *container + "/" + *objectName

	client := &http.Client{}

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("X-Auth-Token", *token)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	object := io.Reader(resp.Body)

	return &object, nil
}

func upload2GCP(ctx context.Context, bkt *storage.BucketHandle, objectName *string, object *io.Reader) error {
	wc := bkt.Object(*objectName).NewWriter(ctx)
	if _, err := io.Copy(wc, *object); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	return nil
}
