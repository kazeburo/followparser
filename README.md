# followparser

followparser is a small Go library to help follow and parse log files reliably.
It is designed for use-cases such as monitoring plugins (e.g. mackerel plugins),
log collectors and small utilities that need to resume reading logs across restarts
and when logs rotate.

followparser はログファイルを安定して追従・解析するための小さな Go ライブラリです。
主に mackerel プラグインのような監視用ユーティリティや、ログのローテーションや再起動の
影響を受けずに読み続ける必要があるツール向けに作られています。

## Features / 特徴

- Resume reading from the last committed position using a posfile (per-user).
- Follow rotated logs by locating archived files by inode.
- Correctly handle files where the last line does not end with a newline.
- Configurable maximum read size and buffer behavior to handle large single-line logs.
- Minimal, dependency-light implementation suitable for embedding in small tools.

- 前回の読み取り位置を posfile として保存し、再起動後も読み続けられます。
- ログがローテートされた際に、inode によって過去ログを検索し追従できます。
- 最後の行が改行で終わらない場合でも正しく処理します。
- 単一行が大きいケースに対応するためのバッファ／読み取り上限設定を提供します。
- 依存を最小限に抑えた実装で小さなツールへの組み込みに適しています。

## Quick usage / 使い方（簡単）

As a library, implement the `Callback` interface and call `Parse` on a `Parser`.

```go
cb := &MyParserImpl{}
parser := &followparser.Parser{
    WorkDir:  "/var/tmp", // where pos files are stored
    Callback: cb,
    // optional:
    // ArchiveDir: "/var/log/archive",
    // Silent: true,
}
parsed, err := parser.Parse("myLogPos", "/var/log/myapp.log")
if err != nil {
    // handle error
}
// parsed contains information about files read (start/end pos, rows)
```

ライブラリとして使用する場合は、`Callback` インターフェースを実装して `Parser.Parse` を呼び出します。
主な設定は `WorkDir`（posfile を保存するディレクトリ）と `ArchiveDir`（省略時はログファイルのディレクトリ）が
あります。

### Callback interface / コールバック

The callback must implement:

```go
type Callback interface {
    Parse(b []byte) error
    Finish(duration float64)
}
```

- `Parse` will be invoked for each line read (b does not include the trailing newline).
- `Finish` is called once parsing is completed with the time (seconds) since the pos file was written.

Callback は行ごとに呼ばれ、`Parse` の引数 `b` には改行文字は含まれません。
`Finish` は最後に一度だけ呼ばれ、posfile の保存時刻からの経過秒数が渡されます。

## Testing / テスト

Run unit tests with:

```bash
go test ./...
```

CI では `go test` を実行して全テストを確認してください。

## License / ライセンス

This project is licensed under the MIT License. See the `LICENSE` file for details.

本プロジェクトは MIT ライセンスの下で公開されています。詳細は `LICENSE` を参照してください。
