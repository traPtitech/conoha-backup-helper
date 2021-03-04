package main

import (
	"context"
	"fmt"
	"strconv"

	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/golang/snappy"
	"github.com/ncw/swift"
	"go.uber.org/atomic"
	"google.golang.org/api/option"

	"cloud.google.com/go/storage"
)

const dateFormat = "2006/01/02 15:04:05"

var greenFmt = color.New(color.FgGreen)
var redFmt = color.New(color.FgRed)

var projectID = os.Getenv("PROJECT_ID")
var parallelNum = 5

func main() {
	if pn := os.Getenv("PARALLEL_NUM"); pn != "" {
		pnI, err := strconv.Atoi(pn)
		if err != nil {
			log.Fatal(err)
		}
		parallelNum = pnI
	}

	path := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")

	objectStorage := &swift.Connection{
		UserName: os.Getenv("CONOHA_USERNAME"),
		ApiKey:   os.Getenv("CONOHA_PASSWORD"),
		AuthUrl:  "https://identity.tyo1.conoha.io/v2.0",
		Tenant:   os.Getenv("CONOHA_TENANT_NAME"),
		TenantId: os.Getenv("CONOHA_TENANT_ID"),
	}
	if err := objectStorage.Authenticate(); err != nil {
		log.Fatal(err)
	}

	containers, err := objectStorage.ContainerNamesAll(nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(containers)

	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(path))
	if err != nil {
		log.Fatal(err)
	}

	backupStart := time.Now()
	totalObjects := 0
	totalErrors := 0

	limit := make(chan bool, parallelNum)
	for _, container := range containers {
		fmt.Println()
		greenFmt.Printf("Creating bucket for %s\n", container)
		bkt, err := createBucket(ctx, client, container)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println()
		greenFmt.Printf("Transferring objects in %s", container) // 下に続く
		objects, err := objectStorage.ObjectNamesAll(container, nil)
		if err != nil {
			log.Fatal(err)
		}
		objectLen := len(objects)
		greenFmt.Printf(": %d objects\n", objectLen)
		totalObjects += objectLen

		var wg sync.WaitGroup
		var doneCount atomic.Uint64
		var errs threadSafeBackupErrorSlice

		for _, objectName := range objects {
			wg.Add(1)

			go func(objectName string) {
				limit <- true
				defer func() { <-limit }()

				defer wg.Done()
				defer func() {
					dc := doneCount.Inc()
					if dc%1000 == 0 {
						fmt.Printf("Done %d of %d\n", dc, objectLen)
					}
				}()

				backupObject(ctx, bkt, objectStorage, container, objectName, &errs)
			}(objectName)
		}
		wg.Wait()

		errCount := errs.len()
		if errCount > 0 {
			redFmt.Printf("Failed to backup %d objects.\n", errCount)
			errStrs := errs.getFormattedErrors()
			fmt.Print(errStrs)
		}
		totalErrors += errCount

	}

	backupEnd := time.Now()
	backupDuration := backupEnd.Sub(backupStart)

	message := fmt.Sprintf(`### オブジェクトストレージのバックアップが保存されました
	バックアップ開始時刻: %s
	バックアップ所要時間: %f
	オブジェクト数: %d
	エラー数: %d
	`, backupStart.Format(dateFormat), backupDuration.Hours(), totalObjects, totalErrors)

	err = postWebhook(message)
	if err != nil {
		fmt.Println("Failed to post webhook:", err)
	}
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

	if err := bkt.Create(ctx, projectID, &bktAttrs); err != nil {
		return nil, err
	}

	greenFmt.Printf("Created: %s\n", bucketName)
	return bkt, nil
}

func transferObject(objectStorage *swift.Connection, container string, objectName string, wc *storage.Writer) error {
	pr, pw := io.Pipe()

	errChan := make(chan error, 1)
	go func() {
		defer pw.Close()

		_, err := objectStorage.ObjectGet(container, objectName, pw, true, nil)
		if err != nil {
			errChan <- err
			return
		}

		errChan <- nil
	}()

	snappyWriter := snappy.NewBufferedWriter(wc)
	defer snappyWriter.Close()

	if _, err := io.Copy(snappyWriter, pr); err != nil {
		return err
	}

	if err := <-errChan; err != nil {
		return err
	}

	return nil
}

func backupObject(ctx context.Context, bkt *storage.BucketHandle, objectStorage *swift.Connection, container string, objectName string, errs *threadSafeBackupErrorSlice) {
	wc := bkt.Object(objectName).NewWriter(ctx)
	defer func() {
		if err := wc.Close(); err != nil {
			errs.append(backupError{
				err:        err,
				objectName: objectName,
			})
		}
	}()

	err := transferObject(objectStorage, container, objectName, wc)
	if err != nil {
		errs.append(backupError{
			err:        err,
			objectName: objectName,
		})
	}
}
