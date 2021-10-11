package main

import (
	"context"
	"errors"
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
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/option"

	"cloud.google.com/go/storage"
)

const dateFormat = "2006/01/02 15:04:05"

var (
	greenFmt = color.New(color.FgGreen)
	redFmt   = color.New(color.FgRed)
)

var (
	projectID              = os.Getenv("PROJECT_ID")
	bucketNameSuffix       = os.Getenv("BUCKET_NAME_SUFFIX")
	parallelNum      int64 = 5
)

func main() {
	if pn := os.Getenv("PARALLEL_NUM"); pn != "" {
		pnI, err := strconv.ParseInt(pn, 10, 64)
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

	// 途中でエラーが起きないでほしいので先に作成しておく
	for _, container := range containers {
		suffixedContainer := container + bucketNameSuffix
		fmt.Println()
		greenFmt.Printf("Ensuring bucket for %s\n", container)
		_, err := ensureBucket(ctx, client, suffixedContainer)
		if err != nil {
			log.Fatal(err)
		}
	}

	backupStart := time.Now()
	totalObjects := 0
	totalErrors := 0
	limit := semaphore.NewWeighted(parallelNum)

	for _, container := range containers {
		suffixedContainer := container + bucketNameSuffix
		bkt := client.Bucket(suffixedContainer)

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
			limit.Acquire(ctx, 1)

			go func(objectName string) {
				defer limit.Release(1)
				defer wg.Done()

				backupObject(ctx, bkt, objectStorage, container, objectName, &errs)

				dc := doneCount.Inc()
				if dc%1000 == 0 {
					fmt.Printf("Done %d of %d\n", dc, objectLen)
				}
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
	バックアップ所要時間: %f時間
	オブジェクト数: %d
	エラー数: %d
	`, backupStart.Format(dateFormat), backupDuration.Hours(), totalObjects, totalErrors)

	err = postWebhook(message)
	if err != nil {
		fmt.Println("Failed to post webhook:", err)
	}
}

func ensureBucket(ctx context.Context, client *storage.Client, container string) (*storage.BucketHandle, error) {
	bkt := client.Bucket(container)

	bktAttrs, err := bkt.Attrs(ctx)
	if err == storage.ErrBucketNotExist {
		createBktAttrs := storage.BucketAttrs{
			StorageClass:      "COLDLINE",
			Location:          "asia-northeast1",
			VersioningEnabled: true,
			// 生成から90日でオブジェクトを削除
			Lifecycle: storage.Lifecycle{Rules: []storage.LifecycleRule{
				{
					Action:    storage.LifecycleAction{Type: "Delete"},
					Condition: storage.LifecycleCondition{AgeInDays: 90},
				},
			}},
		}
		if err := bkt.Create(ctx, projectID, &createBktAttrs); err != nil {
			return nil, err
		}
		greenFmt.Printf("Created: %s\n", container)
	} else if err != nil {
		return nil, err
	} else {
		// already exists

		if bktAttrs.StorageClass != "COLDLINE" {
			return nil, errors.New("Bucket default storage class is not correct. Expected: COLDLINE. Actual: " + bktAttrs.StorageClass)
		}
		if !bktAttrs.VersioningEnabled {
			return nil, errors.New("Bucket versioning status is not correct. Expected: enabled. Actual: disabled")
		}

		// may check more?
	}
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

	err := transferObject(objectStorage, container, objectName, wc)

	wcErr := wc.Close()

	if err != nil {
		errs.append(backupError{
			err:        err,
			objectName: objectName,
		})
	} else if wcErr != nil {
		// オブジェクト一つに対してエラーは一つだけ追加したいため
		// errが存在するときはerrsに追加しない
		errs.append(backupError{
			err:        wcErr,
			objectName: objectName,
		})
	}
}
