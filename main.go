package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"google.golang.org/api/option"

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

var greenFmt = color.New(color.FgGreen)

var tenantID string

func main() {
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)

	path := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	tenantID = os.Getenv("CONOHA_TENANT_ID")

	authInfo := &AuthInfo{}
	authInfo.Auth.PasswordCredentials.UserName = os.Getenv("CONOHA_USERNAME")
	authInfo.Auth.PasswordCredentials.Password = os.Getenv("CONOHA_PASSWORD")
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

	containers, err := retrieveContainerList(token)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(containers)

	limit := make(chan bool, cpus)
	for _, container := range containers {
		fmt.Println()
		greenFmt.Printf("Creating bucket for %s\n", container)
		bkt, err := createBucket(ctx, client, container)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println()
		greenFmt.Printf("Transferring objects in %s", container) // 下に続く
		objects, totalCount, err := retrieveFullObjectList(token, container)
		if err != nil {
			log.Fatal(err)
		}
		greenFmt.Printf(": %d objects\n", totalCount)

		var wg sync.WaitGroup
		for _, objectName := range objects {
			wg.Add(1)
			go backupObject(ctx, bkt, token, container, objectName, &wg, limit)
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
	if err != nil {
		return "", err
	}

	respJSON := &AccessToken{}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&respJSON); err != nil {
		return "", err
	}

	return respJSON.Access.Token.ID, nil
}

func retrieveContainerList(token string) ([]string, error) {
	url := "https://object-storage.tyo1.conoha.io/v1/nc_" + tenantID

	client := &http.Client{}

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("X-Auth-Token", token)

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

func createBucket(ctx context.Context, client *storage.Client, container string) (*storage.BucketHandle, error) {
	t := time.Now()
	bucketName := fmt.Sprintf("%s-%d-%d-%d", container, t.Year(), t.Month(), t.Day())

	bkt := client.Bucket(bucketName)
	bktAttrs := storage.BucketAttrs{
		StorageClass: "COLDLINE",
		Location:     "asia-northeast1",
		// 生成から90日でバケットを削除
		Lifecycle: storage.Lifecycle{Rules: []storage.LifecycleRule{
			{
				Action:    storage.LifecycleAction{Type: "Delete"},
				Condition: storage.LifecycleCondition{AgeInDays: 90},
			},
		}},
	}

	if err := bkt.Create(ctx, os.Getenv("PROJECT_ID"), &bktAttrs); err != nil {
		return nil, err
	}

	greenFmt.Printf("Created: %s\n", bucketName)
	return bkt, nil
}

// 一回のリクエストでは最大10000件までしか帰ってこないのですべて取得するために複数回リクエストを送信する
// refs: https://docs.openstack.org/ja/user-guide/cli-swift-large-lists.html
func retrieveFullObjectList(token string, container string) ([]string, int, error) {
	firstList, total, err := retrieveObjectList(token, container, nil)
	if err != nil {
		return nil, 0, err
	}
	list := make([]string, 0, total)
	list = append(list, firstList...)

	fmt.Println("1: ", len(firstList), len(list), total)

	for len(list) < total {
		marker := list[len(list)-1]
		addList, _, err := retrieveObjectList(token, container, &marker)
		if err != nil {
			return nil, 0, err
		}
		list = append(list, addList...)
	}
	return list, total, nil
}

func retrieveObjectList(token string, container string, marker *string) ([]string, int, error) {
	urlStr := "https://object-storage.tyo1.conoha.io/v1/nc_" + tenantID + "/" + container
	if marker != nil {
		urlStr += "?marker=" + url.QueryEscape(*marker)
	}

	client := &http.Client{}

	req, _ := http.NewRequest("GET", urlStr, nil)
	req.Header.Set("X-Auth-Token", token)

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	totalCountStr := resp.Header.Get("X-Container-Object-Count")
	totalCount, err := strconv.Atoi(totalCountStr)
	if err != nil {
		return nil, 0, err
	}

	objects := strings.Split(string(body), "\n")
	objects = objects[:len(objects)-1]

	return objects, totalCount, nil
}

func transferObject(token string, container string, objectName string, wc *storage.Writer) error {
	url := "https://object-storage.tyo1.conoha.io/v1/nc_" + tenantID + "/" + container + "/" + objectName

	client := &http.Client{}

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("X-Auth-Token", token)

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

func backupObject(ctx context.Context, bkt *storage.BucketHandle, token string, container string, objectName string, wg *sync.WaitGroup, limit chan bool) error {
	limit <- true
	defer func() { <-limit }()
	defer fmt.Println(objectName)
	defer wg.Done()
	wc := bkt.Object(objectName).NewWriter(ctx)
	err := transferObject(token, container, objectName, wc)
	return err
}
