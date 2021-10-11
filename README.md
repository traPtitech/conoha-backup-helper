# conoha-backup-helper
オブジェクトストレージをsnappyで圧縮してGCP Storageにバックアップ
完了したときにtraQにWebhookを送信する

## 設定

環境変数でします。

- オブジェクトストレージ関連
  - `CONOHA_USERNAME`
  - `CONOHA_PASSWORD`
  - `CONOHA_TENANT_NAME`
  - `CONOHA_TENANT_ID`
- GCP関連
  - `GOOGLE_APPLICATION_CREDENTIALS`
  - `PROJECT_ID`
  - `BUCKET_NAME_SUFFIX`: バケット名につけるsuffix
    - GCPのバケット名はグローバル(アカウントを跨いで)でユニークである必要があるため
- traQ関連
  - `TRAQ_WEBHOOK_ID`
  - `TRAQ_WEBHOOK_SECRET`
- その他
  - `PARALLEL_NUM`: 同時ダウンロード数
