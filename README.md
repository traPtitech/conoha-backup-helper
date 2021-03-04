# conoha-backup-helper
オブジェクトストレージをsnappyで圧縮してGCP Storageにバックアップ

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
