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
	"time"

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

var tenantID string

func main() {
	path := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	tenantID = os.Getenv("Conoha_TENANT_ID")

	authInfo := new(AuthInfo)
	authInfo.Auth.PasswordCredentials.UserName = os.Getenv("Conoha_USERNAME")
	authInfo.Auth.PasswordCredentials.Password = os.Getenv("Conoha_PASSWORD")
	authInfo.Auth.TenantID = tenantID

	token, err := getConohaAPIToken(authInfo)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(path))
	if err != nil {
		log.Fatal(err)
	}

	containers, err := retrieveContainerList(&token)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(containers)

	limit := make(chan bool, 30) // 最大でgoroutineは30個
	for _, container := range containers {
		fmt.Println("\n" + "\u001b[00;32m" + "Creating bucket for " + container + " \u001b[00m")
		bkt, err := createBucket(ctx, client, &container)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("\n" + "\u001b[00;32m" + "Transferring objects in " + container + " \u001b[00m")
		objects, err := retrieveObjectList(&token, &container)
		if err != nil {
			log.Fatal(err)
		}

		var wg sync.WaitGroup
		for _, objectName := range objects {
			wg.Add(1)
			go backupObject(ctx, bkt, &token, &container, objectName, &wg, &limit)
			if err != nil {
				log.Fatal(err)
			}
		}
		wg.Wait()
	}
}

func getConohaAPIToken(authInfo *AuthInfo) (string, error) {
	url := "https://identity.tyo1.conoha.io/v2.0/tokens"

	client := &http.Client{}

	reqJSON, _ := json.Marshal(*authInfo)
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
	url := "https://object-storage.tyo1.conoha.io/v1/nc_" + tenantID

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

func createBucket(ctx context.Context, client *storage.Client, container *string) (*storage.BucketHandle, error) {
	t := time.Now()
	bucketName := fmt.Sprintf("%s-%d-%d-%d", *container, t.Year(), t.Month(), t.Day())
	defer fmt.Println("\u001b[00;32m" + "Created: " + bucketName + " \u001b[00m")

	bkt := client.Bucket(bucketName)
	if err := bkt.Create(ctx, os.Getenv("PROJECT_ID"), &storage.BucketAttrs{
		StorageClass: "STANDARD",
		Location:     "asia",
	}); err != nil {
		return nil, err
	}
	return bkt, nil
}

func retrieveObjectList(token *string, container *string) ([]string, error) {
	url := "https://object-storage.tyo1.conoha.io/v1/nc_" + tenantID + "/" + *container

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
	url := "https://object-storage.tyo1.conoha.io/v1/nc_" + tenantID + "/" + *container + "/" + *objectName

	client := &http.Client{}

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("X-Auth-Token", *token)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	// TODO: checksum挟む
	defer resp.Body.Close()
	if _, err := io.Copy(wc, resp.Body); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}

	return nil
}

func backupObject(ctx context.Context, bkt *storage.BucketHandle, token *string, container *string, objectName string, wg *sync.WaitGroup, limit *chan bool) error {
	*limit <- true
	defer func() { <-*limit }()
	defer fmt.Println(objectName)
	defer wg.Done()
	wc := bkt.Object(objectName).NewWriter(ctx)
	err := transferObject(token, container, &objectName, wc)
	return err
}
