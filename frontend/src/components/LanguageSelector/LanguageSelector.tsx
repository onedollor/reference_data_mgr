import React from 'react';
import { Select } from 'antd';
import { GlobalOutlined } from '@ant-design/icons';
import { useTranslation } from 'react-i18next';
import './LanguageSelector.less';

const { Option } = Select;

interface Language {
  code: string;
  name: string;
  flag: string;
}

const languages: Language[] = [
  { code: 'en', name: 'English', flag: '🇺🇸' },
  { code: 'fr', name: 'Français', flag: '🇫🇷' },
  { code: 'zh', name: '中文', flag: '🇨🇳' },
];

const LanguageSelector: React.FC = () => {
  const { i18n } = useTranslation();

  const handleLanguageChange = (languageCode: string) => {
    i18n.changeLanguage(languageCode);
  };

  return (
    <div className="language-selector">
      <Select
        value={i18n.language}
        onChange={handleLanguageChange}
        size="middle"
        suffixIcon={<GlobalOutlined />}
        className="language-select"
      >
        {languages.map((lang) => (
          <Option key={lang.code} value={lang.code}>
            <span className="language-option">
              <span className="flag">{lang.flag}</span>
              <span className="name">{lang.name}</span>
            </span>
          </Option>
        ))}
      </Select>
    </div>
  );
};

export default LanguageSelector;