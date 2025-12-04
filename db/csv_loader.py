"""CSV Loader
Loads and parses CSV data for the distributed database.
Includes sample CSV generation for testing.
"""

import csv
import random
from typing import Dict, List, Set, Tuple, Optional
from pathlib import Path


class CSVLoader:
    """CSV data loading and processing"""

    # Sample novel titles for different languages
    SAMPLE_TITLES = {
        "zh": [
            "三体",
            "活着",
            "围城",
            "平凡的世界",
            "白鹿原",
            "红楼梦",
            "西游记",
            "水浒传",
            "三国演义",
            "金瓶梅",
            "朝花夕拾",
            "呐喊",
            "彷徨",
            "狂人日记",
            "阿Q正传",
            "子夜",
            "家",
            "春",
            "秋",
            "雷雨",
        ],
        "ja": [
            "ノルウェイの森",
            "1Q84",
            "海辺のカフカ",
            "風の歌を聴け",
            "羊をめぐる冒険",
            "こころ",
            "坊っちゃん",
            "吾輩は猫である",
            "雪国",
            "伊豆の踊子",
            "破戒",
            "金閣寺",
            "仮面の告白",
            "細雪",
            "痴人の愛",
            "人間失格",
            "斜陽",
            "走れメロス",
            "山月記",
            "舞姫",
        ],
        "ko": [
            "채식주의자",
            "소년이 온다",
            "작별인사",
            "한강",
            "흰",
            "토지",
            "아리랑",
            "태백산맥",
            "무정",
            "상록수",
            "삼대",
            "난장이가 쏘아올린 작은 공",
            "광장",
            "무진기행",
            "서울 1964년 겨울",
            "엄마를 부탁해",
            "살인자의 기억법",
            "7년의 밤",
            "완득이",
            "우리들의 행복한 시간",
        ],
        "ms": [
            "Hikayat Hang Tuah",
            "Sejarah Melayu",
            "Sulalat al-Salatin",
            "Tenggelamnya Kapal Van Der Wijck",
            "Siti Nurbaya",
            "Salina",
            "Interlok",
            "Ranjau Sepanjang Jalan",
            "Anak Mat Lela Gila",
            "Panglima Awang",
            "Bidasari",
            "Merpati Putih Terbang Lagi",
            "Kekasih Electra",
            "Mat Jenin",
            "Lagenda Budak Setan",
            "Leftenan Adnan",
            "Hang Jebat Menderhaka",
            "Penawar Bagi Hati",
            "Menara",
            "Gadis Jolobu",
        ],
        "fil": [
            "Noli Me Tangere",
            "El Filibusterismo",
            "Florante at Laura",
            "Banaag at Sikat",
            "Luha ng Buwaya",
            "Dekada '70",
            "Mga Ibong Mandaragit",
            "Ilustrado",
            "Smaller and Smaller Circles",
            "Ang Paboritong Libro ni Hudas",
            "Mga Kuwento ni Lola Basyang",
            "ABNKKBSNPLAko?!",
            "Sa Mga Kuko ng Liwanag",
            "Gapo",
            "The Rosales Saga",
            "Padre Faura Witness the Execution of Rizal",
            "Dead Stars",
            "Footnote to Youth",
            "How My Brother Leon Brought Home a Wife",
            "May Day Eve",
        ],
        "id": [
            "Bumi Manusia",
            "Anak Semua Bangsa",
            "Jejak Langkah",
            "Rumah Kaca",
            "Perburuan",
            "Siti Nurbaya",
            "Layar Terkembang",
            "Atheis",
            "Belenggu",
            "Ronggeng Dukuh Paruk",
            "Cantik Itu Luka",
            "Lelaki Harimau",
            "Pulang",
            "Laskar Pelangi",
            "Sang Pemimpi",
            "Ayah",
            "Cinta di Dalam Gelas",
            "Supernova",
            "Negeri 5 Menara",
            "Hujan",
        ],
        "km": [
            "First They Killed My Father",
            "The Lost Executioner",
            "Never Fall Down",
            "In the Shadow of the Banyan",
            "Stay Alive, My Son",
            "The Gate",
            "When Broken Glass Floats",
            "To Destroy You Is No Loss",
            "Children of Cambodia's Killing Fields",
            "The Stones Cry Out",
            "Lucky Child",
            "Survival in the Killing Fields",
            "Cambodia's Curse",
            "Voices from S-21",
            "A Cambodian Prison Portrait",
            "The Death of Vishnu",
            "Bamboo People",
            "Little Princes",
            "Golden Bones",
            "Rice and Baguette",
        ],
        "th": [
            "จดหมายจากหมาเบื่อ",
            "ด้วยรักและผูกพัน",
            "คำพิพากษา",
            "ลูกอีสาน",
            "สี่แผ่นดิน",
            "ขุนช้างขุนแผน",
            "พระอภัยมณี",
            "อิเหนา",
            "ศรีบูรพา",
            "กุหลาบป่า",
            "ชาติหน้าขอให้พบกัน",
            "เลือดมังกร",
            "คนละฟากฟ้า",
            "ข้าหลวง",
            "ตำนานสมเด็จพระนารายณ์",
            "เสือคำราม",
            "สุภาพบุรุษจุฑาเทพ",
            "มัจจุราชเริงรำ",
            "รักไม่มีสิทธิ์",
            "ฟ้าใหม่",
        ],
    }

    @staticmethod
    def load_csv(csv_path: str, encoding: str = "utf-8") -> List[Dict[str, str]]:
        """
        Load CSV and return list of novel dictionaries

        Args:
            csv_path: Path to CSV file
            encoding: File encoding (default UTF-8)

        Returns: List of {"title": str, "original_language": str} dicts
        """
        if not Path(csv_path).exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        novels = []

        with open(csv_path, "r", encoding=encoding) as f:
            reader = csv.DictReader(f)

            for row in reader:
                # Handle different column name variations
                title = row.get("Title") or row.get("title")
                language = (
                    row.get("Original Language")
                    or row.get("original_language")
                    or row.get("language")
                )

                if title and language:
                    novels.append(
                        {"title": title.strip(), "original_language": language.strip()}
                    )

        return novels

    @staticmethod
    def group_by_language(novels: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Group novels by original_language

        Args:
            novels: List of novel dictionaries

        Returns: {language: [novels]} dict
        """
        grouped = {}

        for novel in novels:
            lang = novel["original_language"]
            if lang not in grouped:
                grouped[lang] = []
            grouped[lang].append(novel)

        return grouped

    @staticmethod
    def validate_languages(
        novels: List[Dict], expected: Set[str]
    ) -> Tuple[bool, List[str]]:
        """
        Validate all languages are in expected set

        Args:
            novels: List of novel dictionaries
            expected: Set of expected language codes

        Returns: (all_valid, list_of_unknown_languages)
        """
        found_languages = set(n["original_language"] for n in novels)
        unknown = found_languages - expected

        return (len(unknown) == 0, list(unknown))

    @staticmethod
    def create_sample_csv(
        output_path: str,
        num_records: int = 10000,
        languages: Optional[List[str]] = None,
    ) -> None:
        """
        Generate sample CSV for testing with diverse titles

        Args:
            output_path: Path for output CSV file
            num_records: Number of records to generate
            languages: List of language codes (default: all 8)
        """
        if languages is None:
            languages = ["zh", "ja", "ko", "ms", "fil", "id", "km", "th"]

        # Calculate records per language (evenly distributed)
        records_per_lang = num_records // len(languages)
        remainder = num_records % len(languages)

        novels = []

        for i, lang in enumerate(languages):
            # Add extra records to first languages to account for remainder
            count = records_per_lang + (1 if i < remainder else 0)

            # Get sample titles for this language
            base_titles = CSVLoader.SAMPLE_TITLES.get(
                lang, [f"Novel_{j}" for j in range(20)]
            )

            for j in range(count):
                # Create unique titles by combining base titles with variations
                base_title = base_titles[j % len(base_titles)]

                # Add variations to create more unique titles
                if j >= len(base_titles):
                    variation = j // len(base_titles)
                    if variation == 1:
                        title = f"{base_title} - Part {(j % 10) + 1}"
                    elif variation == 2:
                        title = f"{base_title}: Volume {(j % 5) + 1}"
                    elif variation == 3:
                        title = f"{base_title} ({(j % 20) + 2000})"
                    else:
                        title = f"{base_title} #{j + 1}"
                else:
                    title = base_title

                novels.append({"title": title, "original_language": lang})

        # Shuffle to mix languages
        random.shuffle(novels)

        # Write to CSV
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["Title", "Original Language"])
            writer.writeheader()

            for novel in novels:
                writer.writerow(
                    {
                        "Title": novel["title"],
                        "Original Language": novel["original_language"],
                    }
                )

        print(f"Generated sample CSV: {output_path} ({num_records} records)")

    @staticmethod
    def get_statistics(novels: List[Dict]) -> Dict:
        """
        Get statistics about novel dataset

        Args:
            novels: List of novel dictionaries

        Returns: Statistics dictionary
        """
        grouped = CSVLoader.group_by_language(novels)

        stats = {
            "total_records": len(novels),
            "unique_languages": len(grouped),
            "by_language": {
                lang: len(novels_list) for lang, novels_list in grouped.items()
            },
            "min_per_language": min(len(v) for v in grouped.values()) if grouped else 0,
            "max_per_language": max(len(v) for v in grouped.values()) if grouped else 0,
            "avg_per_language": len(novels) / len(grouped) if grouped else 0,
        }

        return stats


# Convenience functions
def load_csv(csv_path: str) -> List[Dict[str, str]]:
    """Convenience function for loading CSV"""
    return CSVLoader.load_csv(csv_path)


def group_by_language(novels: List[Dict]) -> Dict[str, List[Dict]]:
    """Convenience function for grouping"""
    return CSVLoader.group_by_language(novels)


def create_sample_csv(
    output_path: str, num_records: int = 10000, languages: Optional[List[str]] = None
) -> None:
    """Convenience function for creating sample CSV"""
    CSVLoader.create_sample_csv(output_path, num_records, languages)
