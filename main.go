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
	"sync"

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
	fmt.Print(containers)

	// TODO: コンテナごとにバケットを分割
	for _, container := range containers {
		fmt.Print("Transferring objects of " + container + ": ")

		objects, err := retrieveObjectList(&token, &container)
		if err != nil {
			log.Fatal(err)
		}

		// TODO: スレッドの上限設定
		var wg sync.WaitGroup
		for _, objectName := range objects {
			wg.Add(1)
			backupObject(ctx, bkt, &token, &container, &objectName, &wg)
			if err != nil {
				log.Fatal(err)
			}
		}
		wg.Wait()

		fmt.Println("Done!")
	}
}

func getConohaAPIToken() (string, error) {
	url := "https://identity.tyo1.conoha.io/v2.0/tokens"
	authInfo := new(AuthInfo)
	authInfo.Auth.PasswordCredentials.UserName = os.Getenv("Conoha_USERNAME")
	authInfo.Auth.PasswordCredentials.Password = os.Getenv("Conoha_PASSWORD")
	authInfo.Auth.TenantID = os.Getenv("Conoha_TENANT_ID")

	client := &http.Client{}

	reqJSON, _ := json.Marshal(authInfo)
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

	err = json.Unmarshal(body, respJSON)
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
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)

	containers := strings.Split(buf.String(), "\n")
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
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)

	objects := strings.Split(buf.String(), "\n")
	objects = objects[:len(objects)-1]

	return objects, nil
}

func transferObject(token *string, container *string, objectName *string, wc *storage.Writer) error {
	url := "https://object-storage.tyo1.conoha.io/v1/nc_" + os.Getenv("Conoha_TENANT_ID") + "/" + *container + "/" + *objectName

	client := &http.Client{}

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("X-Auth-Token", *token)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	if _, err := io.Copy(wc, resp.Body); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}

	return nil
}

func backupObject(ctx context.Context, bkt *storage.BucketHandle, token *string, container *string, objectName *string, wg *sync.WaitGroup) error {
	defer wg.Done()
	wc := bkt.Object(*objectName).NewWriter(ctx)
	err := transferObject(token, container, objectName, wc)
	return err
}
