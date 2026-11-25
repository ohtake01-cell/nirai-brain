# ニライBrain 頭脳単体貸し出し・スタンドアロン運用ガイド（`cpu_agent` モード）

- **対象版**: 第19波〜第20波（二本柱アーキテクチャ・MCP/CLI `brain_mode` 対応・`_resolve_top3_items` O(1) 高速化済み）
- **位置づけ**: GPU・ローカル AI サーバー（Ollama / Reranker / bge-m3）を一切貸し出さず、**頭脳ソフトウェア本体だけ**を別マシン・別プロジェクト・軽量環境へ持ち出して運用するための公式手順書。

---

## 1. 二本柱アーキテクチャの使い分け

| モード | 指定値 | ローカル AI / GPU | 既定候補数 | 役割と向いている場面 |
|---|---|---|---:|---|
| **① ローカル AI あり（既定）** | `auto`（または省略） | 必要（`bge-m3`＋リランカー） | 3 件（文脈 12 件） | 王国本番（`newpc`）など GPU 常駐環境での最大精度運用 |
| **② ローカル AI なし（純 CPU）** | `cpu_agent`（別名: `cpu`, `no_local_ai`, `cpu_only`） | **不要（ポート接続 0 件）** | **12 件**（広め拾い） | **頭脳単体貸し出し**・ノート PC・軽量 VPS・オフライン環境。純 CPU 資産（TAAT BM25・主題台帳・訂正履歴・鮮度・時間軸・観点字面）で広めに拾い、4 段階 `agent_confidence` を添えて最終判断をエージェントへ委ねる |

---

## 2. 必要最小 PC 要件・対応環境

- **GPU**: 不要（VRAM 0 MiB）
- **Python**: Python 3.10 以上（**標準ライブラリのみ・`pip install` 不要・外部パッケージ依存 0 件**）
- **メモリ**: RAM 4 GB 以上（推奨 8 GB）
- **ストレージ**: SSD 空き 1 GB 以上（頭脳コード一式は 12 ファイル合計 約 **915 KB**）
- **対応 OS**:
  - **macOS**（Apple Silicon / Intel）: 無改造で完全動作
  - **Linux**: 無改造で完全動作
  - **Windows (WSL2)**: 無改造で完全動作
  - **非 WSL ネイティブ Windows（制約事項）**: 現行ファイルロックに Unix 標準モジュール `fcntl` を使用しているため未対応。Windows 環境では **WSL2** 上での実行を推奨します。

---

## 3. 最小パッケージ構成（12 ファイル・外部依存ゼロ）

`python3 -S`（site-packages 完全無効）かつソケット接続全面遮断の隔離環境で、スナップショット生成・検索・コンテキスト構築・監査の完走を機械実証済みの最小集合です：

1. `scripts/roundtable_brain.py`（共有脳コア・スナップショット生成・検索オーケストレーション）
2. `scripts/brain_cpu_agent.py`（`cpu_agent` モード制御・`ContextVar` AI 遮断・4 段階 `agent_confidence`・特異語被覆率ガード）
3. `scripts/brain_bm25.py`（TAAT BM25 転置索引）
4. `scripts/brain_text_utils.py`（正規化・クエリ語分解・キー生成）
5. `scripts/brain_speed_cache.py`（CLI `main_impl`・遅延 `shiage`・`render_context`・鮮度キャッシュ）
6. `scripts/brain_evolve_rank.py`（進化スコア・観点/主題の席補助）
7. `scripts/brain_review_triage.py`（レビュー抽出・ノイズ選別）
8. `scripts/ai_availability.py`（稼働状態オーバーレイ）
9. `scripts/nirai_record_roles.py`（記録ロール付与）
10. `scripts/roundtable_event.py`（円卓イベント正本読み取り・名義正規化）
11. `scripts/roundtable_media.py`（添付メディア索引）
12. `scripts/kingdom_read_acceleration.py`（イベント高速読み込み）

※任意で `--with-optional` を指定すると、`brain_no_sendo.py`・`fact_layer.py`・`shussho.py` の 3 ファイルを追加同梱して主題台帳・出所札を強化できます（これらも標準ライブラリのみ）。

---

## 4. 書き出し＆完全オフライン検証（1 コマンド）

```bash
# 1) 任意ディレクトリへ頭脳単体パッケージを書き出す（サンプル記録付き）
python3 scripts/brain_standalone_pack.py export --dest /path/to/nirai-brain-standalone --init-sample

# 2) 書き出したパッケージが外部pip依存0・ソケット接続0で完走するか自己検証する
python3 scripts/brain_standalone_pack.py verify --dir /path/to/nirai-brain-standalone
```

---

## 5. 貸し出し先マシンでの 3 ステップ利用手順

書き出したディレクトリ（`/path/to/nirai-brain-standalone`）を別マシンへコピーしたあと：

```bash
cd /path/to/nirai-brain-standalone

# 1. イベント記録（roundtable/events/*.jsonl）からスナップショットを生成
python3 scripts/roundtable_brain.py --root . refresh

# 2. 純CPU広め拾いモード（cpu_agent）で検索（JSON出力・既定12件＋agent_confidence）
python3 scripts/roundtable_brain.py --root . search --query "知りたい主題" --mode cpu_agent

# 3. エージェントへそのまま渡せる Markdown コンテキストを生成
python3 scripts/roundtable_brain.py --root . context --query "知りたい主題" --mode cpu_agent --format markdown
```

### 指定方法の使い分け（CLI / MCP / 環境変数）

| 入口 | `cpu_agent` 指定方法 | 既定動作（未指定時） |
|---|---|---|
| **環境変数（全入口共通）** | `export KINGDOM_BRAIN_MODE=cpu_agent` | 未設定なら `auto`（GPU 最大精度） |
| **CLI (`search` / `context`)** | `--mode cpu_agent`（または `--brain-mode cpu_agent`） | 環境変数に従う（既定 `auto`） |
| **MCP `roundtable_brain_search`** | `brain_mode="cpu_agent"`（`limit=0` 省略時は自動で 12 件以上へ拡張） | 既存と完全同一（`auto`） |
| **MCP `roundtable_brain_context`** | `brain_mode="cpu_agent"`（`limit=0` 省略時は 12 件以上を確保） | 既存と完全同一（`auto`） |

---

## 6. 4 段階 `agent_confidence` の読み方とエージェント行動指針

`cpu_agent` モードの検索・コンテキスト応答には、コーパス規模に依存しない完全スケール不変 `[0.0..1.0]` の信頼度サイン `agent_confidence` が添付されます（クエリ文字列やトークン本文を含まない非リーク構造）：

| `level` | 意味 | エージェント側の推奨行動 |
|---|---|---|
| **`high`** | 上位候補に高い語彙・特異語一致あり（第19波 376 問実測で陰性混入 0%） | 上位候補の本文・日付・訂正履歴（`supersedes`）を確認して根拠付き回答を作成 |
| **`medium`** | 上位候補群に関連記録を含む可能性が高い（同実測で陰性混入 0%） | 上位候補（Top-12）を読み比べて最終判断 |
| **`low`** | 語彙一致が薄い | 候補本文を鵜呑みにせず、別キーワード・同義語での再検索を推奨 |
| **`likely_absent`** | 共有脳に該当記録が存在しない可能性が高い（同実測でハード陰性 91.7% 直撃） | 候補をそのまま根拠に使わず、別表現で 1 回確認した上で「記録なし」と判断 |
