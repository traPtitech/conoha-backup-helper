package main

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

var (
	webhookId     = os.Getenv("TRAQ_WEBHOOK_ID")
	webhookSecret = os.Getenv("TRAQ_WEBHOOK_SECRET")
)

func postWebhook(message string) error {
	url := "https://q.trap.jp/api/v3/webhooks/" + webhookId
	sig := generateSignature(message)

	req, err := http.NewRequest("POST", url, strings.NewReader(message))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("X-TRAQ-Signature", sig)

	res, err := http.DefaultClient.Do(req)
	defer res.Body.Close()
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	fmt.Printf("Sent webhook to traQ: statusCode: %d, body: %s\n", res.StatusCode, body)
	return nil
}

func generateSignature(message string) string {
	mac := hmac.New(sha1.New, []byte(webhookSecret))
	_, _ = mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}
