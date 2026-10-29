import { defineConfig } from 'vitepress'

const base = process.env.WS_FLOW_DOCS_BASE || '/'

export default defineConfig({
  lang: 'zh-CN',
  title: 'ws-flow',
  description:
    '让双向通信有条不紊。面向事件订阅、双向请求与流式任务的 WebSocket 通信库。',
  srcDir: 'manual',
  base,
  head: [
    ['meta', { name: 'theme-color', content: '#f8faf8' }],
    ['link', { rel: 'icon', type: 'image/svg+xml', href: `${base}flow-mark.svg` }]
  ],
  lastUpdated: false,
  markdown: { languageAlias: { mjs: 'js' } },
  themeConfig: {
    logo: '/flow-mark.svg',
    siteTitle: 'ws-flow',
    socialLinks: [{ icon: 'github', link: 'https://github.com/Mr-Pardon/ws-flow' }],
    nav: [
      { text: '指南', link: '/guide/quick-start', activeMatch: '/guide/' },
      { text: 'API', link: '/reference/', activeMatch: '/reference/' },
      { text: '案例', link: '/examples/', activeMatch: '/examples/' },
      { text: '架构', link: '/design/', activeMatch: '/design/' }
    ],
    sidebar: [
      {
        text: '开始使用',
        items: [
          { text: '认识 ws-flow', link: '/guide/' },
          { text: '快速开始', link: '/guide/quick-start' }
        ]
      },
      {
        text: '通信指南',
        items: [
          { text: '事件与订阅', link: '/guide/events' },
          { text: '双向请求', link: '/guide/requests' },
          { text: '流式任务', link: '/guide/streams' },
          { text: '连接与生命周期', link: '/guide/lifecycle' },
          { text: 'Server 与连接组织', link: '/guide/server' },
          { text: '按需扩展', link: '/guide/extensions' }
        ]
      },
      {
        text: '参考手册',
        items: [
          { text: 'API 导览', link: '/reference/' },
          { text: 'Client', link: '/reference/client' },
          { text: 'Server 与 Session', link: '/reference/server' },
          { text: '错误与排查', link: '/reference/errors' },
          { text: '资源与期限配置', link: '/reference/resources' }
        ]
      },
      {
        text: '完整案例',
        items: [
          { text: '选择一个场景', link: '/examples/' },
          { text: '独立通用客户端', link: '/examples/generic-client' },
          { text: '可取消的进度任务', link: '/examples/cancellable-task' },
          { text: '双向调用与结果收集', link: '/examples/duplex' }
        ]
      },
      {
        text: '架构与参与',
        items: [
          { text: '架构模型', link: '/design/' },
          { text: '参与开发', link: '/design/contributing' },
          { text: '版本升级', link: '/reference/migration' }
        ]
      }
    ],
    outline: { label: '本页内容', level: [2, 3] },
    docFooter: { prev: '上一页', next: '下一页' },
    sidebarMenuLabel: '目录',
    returnToTopLabel: '返回顶部',
    darkModeSwitchLabel: '外观',
    lightModeSwitchTitle: '切换到浅色模式',
    darkModeSwitchTitle: '切换到深色模式',
    search: {
      provider: 'local',
      options: {
        locales: {
          root: {
            translations: {
              button: { buttonText: '搜索文档', buttonAriaLabel: '搜索文档' },
              modal: {
                noResultsText: '没有找到相关内容',
                resetButtonTitle: '清除搜索',
                footer: {
                  selectText: '选择',
                  navigateText: '切换',
                  closeText: '关闭'
                }
              }
            }
          }
        }
      }
    },
    footer: {
      message: '以 MIT 许可证开源 · 为清晰、可组合的通信而设计',
      copyright: 'Copyright © 2026 Mr_Pardon'
    }
  }
})
