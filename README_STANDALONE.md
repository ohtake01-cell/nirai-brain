# ニライBrain スタンドアロン最小パッケージ（`cpu_agent` 純CPUモード）

GPU・ローカルAIサーバー・外部 pip パッケージを一切使わず、Python 3.10+ 標準ライブラリのみで
共有記憶のスナップショット生成・BM25/鮮度/訂正履歴検索・4段階 `agent_confidence` 出力を行う最小パッケージです。

## 動作要件
- **OS**: macOS / Linux / Windows (WSL2)
  - ※非 WSL のネイティブ Windows では Unix `fcntl` ファイルロックが未対応のため WSL2 を使用してください。
- **Python**: Python 3.10 以上（標準ライブラリのみ・`pip install` 不要）
- **ハードウェア**: GPU 不要・RAM 4 GB 以上（推奨 8 GB）・SSD 空き 1 GB 以上

## 3 ステップ起動手順

```bash
# 1. イベント記録から共有脳スナップショットを生成
python3 scripts/roundtable_brain.py --root . refresh

# 2. 純CPU広め拾いモード（cpu_agent）で検索（JSON出力・既定12件＋4段階 agent_confidence）
python3 scripts/roundtable_brain.py --root . search --query "検索したい内容" --mode cpu_agent

# 3. エージェント投入用コンテキスト取得（Markdown形式）
python3 scripts/roundtable_brain.py --root . context --query "検索したい内容" --mode cpu_agent --format markdown
```

環境変数 `KINGDOM_BRAIN_MODE=cpu_agent` を設定すれば、`--mode` 省略時も常に純CPUモードで動作します。

## バグ報告と安全アップデート（オプトイン）

```bash
# 不具合時の自動マスク済み診断レポート生成（勝手な外部送信は行いません）
python3 scripts/brain_standalone_pack.py bug-report --root . --summary "症状の要約"

# 診断レポートを確認後、GitHub Issue 起票画面を開く（1クリック報告）
python3 scripts/brain_standalone_pack.py bug-report --root . --summary "症状の要約" --open-github

# 最新リリースの確認・SHA-256検証＆自動ロールバック付きアップデート
python3 scripts/brain_standalone_pack.py update --root . --check-only
python3 scripts/brain_standalone_pack.py update --root .
```

## `agent_confidence.level` の読み方
- `high`: 上位候補に高い語彙・特異語一致あり（本文・日付・訂正履歴を確認して回答）
- `medium`: 上位候補群に関連記録を含む可能性が高い（上位候補を読み比べて判断）
- `low`: 語彙一致が薄い（本文精査と別キーワードでの再検索を推奨）
- `likely_absent`: 共有脳に該当記録が存在しない可能性が高い（別表現で再検索または「記録なし」判断を優先）
